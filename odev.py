import csv
import threading
from typing import List, Dict, Tuple
from dataclasses import dataclass
from copy import deepcopy
import os

@dataclass
class Process:
    pid: str
    arrival_time: int
    burst_time: int
    priority: int
    remaining_time: int = 0
    waiting_time: int = 0
    turnaround_time: int = 0
    completion_time: int = 0
    start_time: int = -1
    
    def __post_init__(self):
        self.remaining_time = self.burst_time

class SchedulerResult:
    def __init__(self):
        self.timeline = []  # (start, end, pid)
        self.context_switches = 0
        self.total_time = 0
        self.processes = []
        
    def calculate_metrics(self):
        waiting_times = [p.waiting_time for p in self.processes]
        turnaround_times = [p.turnaround_time for p in self.processes]
        
        return {
            'max_waiting': max(waiting_times) if waiting_times else 0,
            'avg_waiting': sum(waiting_times) / len(waiting_times) if waiting_times else 0,
            'max_turnaround': max(turnaround_times) if turnaround_times else 0,
            'avg_turnaround': sum(turnaround_times) / len(turnaround_times) if turnaround_times else 0,
        }
    
    def calculate_throughput(self, time_points=[50, 100, 150, 200]):
        throughput = {}
        for t in time_points:
            completed = sum(1 for p in self.processes if p.completion_time <= t)
            throughput[t] = completed
        return throughput
    
    def calculate_cpu_efficiency(self, context_switch_time=0.001):
        busy_time = sum(end - start for start, end, pid in self.timeline if pid != "IDLE")
        total_context_switches = self.context_switches
        overhead = total_context_switches * context_switch_time
        
        if self.total_time > 0:
            efficiency = (busy_time / (self.total_time + overhead)) * 100
        else:
            efficiency = 0
        
        return efficiency

def read_processes(filename: str) -> List[Process]:
    processes = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            p = Process(
                pid=row['ProcessID'],
                arrival_time=int(row['ArrivalTime']),
                burst_time=int(row['BurstTime']),
                priority=int(row['Priority'])
            )
            processes.append(p)
    return processes

def fcfs_scheduler(processes: List[Process]) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    procs.sort(key=lambda x: x.arrival_time)
    
    current_time = 0
    
    for p in procs:
        if current_time < p.arrival_time:
            result.timeline.append((current_time, p.arrival_time, "IDLE"))
            current_time = p.arrival_time
        
        p.start_time = current_time
        p.waiting_time = current_time - p.arrival_time
        current_time += p.burst_time
        p.completion_time = current_time
        p.turnaround_time = p.completion_time - p.arrival_time
        
        result.timeline.append((p.start_time, p.completion_time, p.pid))
        
        if procs.index(p) < len(procs) - 1:
            result.context_switches += 1
    
    result.total_time = current_time
    result.processes = procs
    return result

def preemptive_sjf_scheduler(processes: List[Process]) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    
    current_time = 0
    completed = 0
    n = len(procs)
    ready_queue = []
    last_pid = None
    
    while completed < n:
        # Add arrived processes to ready queue
        for p in procs:
            if p.arrival_time <= current_time and p.remaining_time > 0 and p not in ready_queue:
                ready_queue.append(p)
        
        if ready_queue:
            # Select process with shortest remaining time
            ready_queue.sort(key=lambda x: x.remaining_time)
            current_process = ready_queue[0]
            
            if current_process.start_time == -1:
                current_process.start_time = current_time
            
            # Execute for 1 time unit
            start = current_time
            current_process.remaining_time -= 1
            current_time += 1
            
            # Context switch detection
            if last_pid != current_process.pid and last_pid is not None:
                result.context_switches += 1
            
            result.timeline.append((start, current_time, current_process.pid))
            last_pid = current_process.pid
            
            if current_process.remaining_time == 0:
                current_process.completion_time = current_time
                current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
                current_process.waiting_time = current_process.turnaround_time - current_process.burst_time
                ready_queue.remove(current_process)
                completed += 1
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p.remaining_time > 0], default=current_time)
            if next_arrival > current_time:
                result.timeline.append((current_time, next_arrival, "IDLE"))
                current_time = next_arrival
                last_pid = None
    
    result.total_time = current_time
    result.processes = procs
    return result

def non_preemptive_sjf_scheduler(processes: List[Process]) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    
    current_time = 0
    completed = []
    remaining = procs.copy()
    
    while remaining:
        # Get available processes
        available = [p for p in remaining if p.arrival_time <= current_time]
        
        if not available:
            next_arrival = min(p.arrival_time for p in remaining)
            result.timeline.append((current_time, next_arrival, "IDLE"))
            current_time = next_arrival
            continue
        
        # Select shortest job
        available.sort(key=lambda x: x.burst_time)
        current_process = available[0]
        
        current_process.start_time = current_time
        current_process.waiting_time = current_time - current_process.arrival_time
        current_time += current_process.burst_time
        current_process.completion_time = current_time
        current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
        
        result.timeline.append((current_process.start_time, current_process.completion_time, current_process.pid))
        
        remaining.remove(current_process)
        completed.append(current_process)
        
        if remaining:
            result.context_switches += 1
    
    result.total_time = current_time
    result.processes = procs
    return result

def round_robin_scheduler(processes: List[Process], quantum: int = 4) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    
    current_time = 0
    ready_queue = []
    completed = []
    last_pid = None
    
    # Sort by arrival time
    procs.sort(key=lambda x: x.arrival_time)
    proc_index = 0
    
    while len(completed) < len(procs):
        # Add newly arrived processes
        while proc_index < len(procs) and procs[proc_index].arrival_time <= current_time:
            ready_queue.append(procs[proc_index])
            proc_index += 1
        
        if ready_queue:
            current_process = ready_queue.pop(0)
            
            if current_process.start_time == -1:
                current_process.start_time = current_time
            
            # Context switch detection
            if last_pid != current_process.pid and last_pid is not None:
                result.context_switches += 1
            
            # Execute for quantum or remaining time
            exec_time = min(quantum, current_process.remaining_time)
            start = current_time
            current_process.remaining_time -= exec_time
            current_time += exec_time
            
            result.timeline.append((start, current_time, current_process.pid))
            last_pid = current_process.pid
            
            # Add newly arrived processes before re-queuing
            while proc_index < len(procs) and procs[proc_index].arrival_time <= current_time:
                ready_queue.append(procs[proc_index])
                proc_index += 1
            
            if current_process.remaining_time == 0:
                current_process.completion_time = current_time
                current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
                current_process.waiting_time = current_process.turnaround_time - current_process.burst_time
                completed.append(current_process)
            else:
                ready_queue.append(current_process)
        else:
            # IDLE
            if proc_index < len(procs):
                next_arrival = procs[proc_index].arrival_time
                result.timeline.append((current_time, next_arrival, "IDLE"))
                current_time = next_arrival
                last_pid = None
    
    result.total_time = current_time
    result.processes = procs
    return result

def preemptive_priority_scheduler(processes: List[Process]) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    
    current_time = 0
    completed = 0
    n = len(procs)
    ready_queue = []
    last_pid = None
    
    while completed < n:
        # Add arrived processes
        for p in procs:
            if p.arrival_time <= current_time and p.remaining_time > 0 and p not in ready_queue:
                ready_queue.append(p)
        
        if ready_queue:
            # Select highest priority (lower number = higher priority)
            ready_queue.sort(key=lambda x: x.priority)
            current_process = ready_queue[0]
            
            if current_process.start_time == -1:
                current_process.start_time = current_time
            
            # Execute for 1 time unit
            start = current_time
            current_process.remaining_time -= 1
            current_time += 1
            
            # Context switch detection
            if last_pid != current_process.pid and last_pid is not None:
                result.context_switches += 1
            
            result.timeline.append((start, current_time, current_process.pid))
            last_pid = current_process.pid
            
            if current_process.remaining_time == 0:
                current_process.completion_time = current_time
                current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
                current_process.waiting_time = current_process.turnaround_time - current_process.burst_time
                ready_queue.remove(current_process)
                completed += 1
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p.remaining_time > 0], default=current_time)
            if next_arrival > current_time:
                result.timeline.append((current_time, next_arrival, "IDLE"))
                current_time = next_arrival
                last_pid = None
    
    result.total_time = current_time
    result.processes = procs
    return result

def non_preemptive_priority_scheduler(processes: List[Process]) -> SchedulerResult:
    result = SchedulerResult()
    procs = deepcopy(processes)
    
    current_time = 0
    completed = []
    remaining = procs.copy()
    
    while remaining:
        # Get available processes
        available = [p for p in remaining if p.arrival_time <= current_time]
        
        if not available:
            next_arrival = min(p.arrival_time for p in remaining)
            result.timeline.append((current_time, next_arrival, "IDLE"))
            current_time = next_arrival
            continue
        
        # Select highest priority
        available.sort(key=lambda x: x.priority)
        current_process = available[0]
        
        current_process.start_time = current_time
        current_process.waiting_time = current_time - current_process.arrival_time
        current_time += current_process.burst_time
        current_process.completion_time = current_time
        current_process.turnaround_time = current_process.completion_time - current_process.arrival_time
        
        result.timeline.append((current_process.start_time, current_process.completion_time, current_process.pid))
        
        remaining.remove(current_process)
        completed.append(current_process)
        
        if remaining:
            result.context_switches += 1
    
    result.total_time = current_time
    result.processes = procs
    return result

def write_results(algorithm_name: str, result: SchedulerResult, filename: str):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"=== {algorithm_name} ===\n\n")
        
        # a) Timeline
        f.write("a) Zaman Tablosu:\n")
        for start, end, pid in result.timeline:
            f.write(f"[{start:3d}] - - {pid:8s} - - [{end:3d}]\n")
        f.write("\n")
        
        # b) Waiting Time
        metrics = result.calculate_metrics()
        f.write("b) Bekleme Süresi:\n")
        f.write(f"   Maksimum: {metrics['max_waiting']:.2f}\n")
        f.write(f"   Ortalama: {metrics['avg_waiting']:.2f}\n\n")
        
        # c) Turnaround Time
        f.write("c) Tamamlanma Süresi:\n")
        f.write(f"   Maksimum: {metrics['max_turnaround']:.2f}\n")
        f.write(f"   Ortalama: {metrics['avg_turnaround']:.2f}\n\n")
        
        # d) Throughput
        throughput = result.calculate_throughput()
        f.write("d) İş Tamamlama Sayısı (Throughput):\n")
        for t, count in throughput.items():
            f.write(f"   T={t}: {count} süreç\n")
        f.write("\n")
        
        # e) CPU Efficiency
        efficiency = result.calculate_cpu_efficiency()
        f.write("e) Ortalama CPU Verimliliği:\n")
        f.write(f"   {efficiency:.2f}%\n\n")
        
        # f) Context Switches
        f.write("f) Toplam Bağlam Değiştirme Sayısı:\n")
        f.write(f"   {result.context_switches}\n\n")
        
        # Process details
        f.write("Süreç Detayları:\n")
        f.write(f"{'PID':<10} {'Varış':<8} {'İşlem':<8} {'Öncelik':<10} {'Bekleme':<10} {'Tamamlanma':<12}\n")
        for p in result.processes:
            f.write(f"{p.pid:<10} {p.arrival_time:<8} {p.burst_time:<8} {p.priority:<10} "
                   f"{p.waiting_time:<10} {p.turnaround_time:<12}\n")

def run_algorithm(algorithm_name, algorithm_func, processes, output_file, results_dict):
    print(f"Çalıştırılıyor: {algorithm_name}")
    result = algorithm_func(processes)
    write_results(algorithm_name, result, output_file)
    results_dict[algorithm_name] = result
    print(f"Tamamlandı: {algorithm_name}")

def main():
    # CSV dosya adını buraya girin
    csv_file = "processes.csv"
    
    if not os.path.exists(csv_file):
        print(f"UYARI: '{csv_file}' dosyası bulunamadı!")
        print("Örnek CSV dosyası oluşturuluyor...")
        
        # Örnek veri oluştur
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ProcessID', 'ArrivalTime', 'BurstTime', 'Priority'])
            writer.writerow(['P001', 0, 8, 2])
            writer.writerow(['P002', 1, 4, 1])
            writer.writerow(['P003', 2, 9, 3])
            writer.writerow(['P004', 3, 5, 2])
            writer.writerow(['P005', 4, 2, 4])
        
        print(f"'{csv_file}' oluşturuldu.\n")
    
    processes = read_processes(csv_file)
    print(f"{len(processes)} süreç yüklendi.\n")
    
    # Algorithm definitions
    algorithms = [
        ("FCFS", fcfs_scheduler, "fcfs_results.txt"),
        ("Preemptive SJF", preemptive_sjf_scheduler, "preemptive_sjf_results.txt"),
        ("Non-Preemptive SJF", non_preemptive_sjf_scheduler, "non_preemptive_sjf_results.txt"),
        ("Round Robin", lambda p: round_robin_scheduler(p, quantum=4), "round_robin_results.txt"),
        ("Preemptive Priority", preemptive_priority_scheduler, "preemptive_priority_results.txt"),
        ("Non-Preemptive Priority", non_preemptive_priority_scheduler, "non_preemptive_priority_results.txt"),
    ]
    
    # BONUS: Run all algorithms concurrently using threads
    threads = []
    results_dict = {}
    
    print("=== BONUS: Tüm algoritmalar eş zamanlı olarak çalıştırılıyor ===\n")
    
    for name, func, output_file in algorithms:
        thread = threading.Thread(
            target=run_algorithm,
            args=(name, func, processes, output_file, results_dict)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    print("\n=== Tüm algoritmalar tamamlandı! ===")
    print("\nSonuç dosyaları:")
    for _, _, output_file in algorithms:
        print(f"  - {output_file}")
    
    # Summary comparison
    print("\n=== Özet Karşılaştırma ===")
    print(f"{'Algoritma':<25} {'Ort. Bekleme':<15} {'Ort. Tamamlanma':<18} {'CPU Verimliliği':<18} {'Bağlam Değ.':<15}")
    print("-" * 90)
    
    for name, _, _ in algorithms:
        if name in results_dict:
            result = results_dict[name]
            metrics = result.calculate_metrics()
            efficiency = result.calculate_cpu_efficiency()
            print(f"{name:<25} {metrics['avg_waiting']:<15.2f} {metrics['avg_turnaround']:<18.2f} "
                  f"{efficiency:<18.2f}% {result.context_switches:<15}")

if __name__ == "__main__":
    main()