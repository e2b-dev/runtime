# fc-debug.gdb — reusable gdb macros for debugging an e2b guest kernel.
#
# Loaded by the init script that `resume-build -gdb` generates (which also does
# `add-symbol-file <vmlinux.debug> -o <slide>` and `target remote <socket>`); this
# file only defines macros, so it carries no socket/symbol/slide assumptions and is
# safe to `source` standalone.
#
# Targets Linux 6.1.x on x86_64 (the e2b guest kernel). Field offsets and per-CPU /
# maple-tree internals are kernel-version specific; the fault-attribution macro is
# the proven workhorse (it reads handle_mm_fault's own arguments and needs no kernel
# layout assumptions), the rest are best-effort conveniences.

# fc-faults [N]
#   Break handle_mm_fault and report the next N guest page faults (default 20) as
#   comm / pid / faulting-address / VMA-range / VMA-flags. handle_mm_fault's args
#   give us everything directly (x86_64 SysV: $rdi = struct vm_area_struct *,
#   $rsi = address), so this works regardless of kernel layout. NULL-guarded.
#   This is the host-invisible signal: which guest process+VMA faulted, during a
#   resume, that UFFD/host telemetry cannot attribute.
define fc-faults
  if $argc == 0
    set $_fc_n = 20
  else
    set $_fc_n = $arg0
  end
  hbreak *handle_mm_fault
  set $_fc_i = 0
  while $_fc_i < $_fc_n
    continue
    set $_fc_vma = (struct vm_area_struct *)$rdi
    set $_fc_addr = $rsi
    if $_fc_vma != 0
      set $_fc_mm = $_fc_vma->vm_mm
      if $_fc_mm != 0
        set $_fc_task = $_fc_mm->owner
        if $_fc_task != 0
          printf "FAULT comm=%s pid=%d addr=0x%lx vma=0x%lx-0x%lx flags=0x%lx\n", $_fc_task->comm, $_fc_task->pid, $_fc_addr, $_fc_vma->vm_start, $_fc_vma->vm_end, $_fc_vma->vm_flags
        end
      end
    end
    set $_fc_i = $_fc_i + 1
  end
end
document fc-faults
Report the next N guest page faults as comm/pid/addr/VMA. Usage: fc-faults [N=20]
Sets a breakpoint on handle_mm_fault; call once per debugging session.
end

# fc-task <task_struct *>
#   Print the essentials of a task_struct pointer: comm, pid, tgid, and its mm.
define fc-task
  set $_t = (struct task_struct *)$arg0
  if $_t == 0
    printf "fc-task: NULL task\n"
  else
    printf "task=0x%lx comm=%s pid=%d tgid=%d mm=0x%lx\n", $_t, $_t->comm, $_t->pid, $_t->tgid, $_t->mm
  end
end
document fc-task
Print comm/pid/tgid/mm for a task_struct pointer. Usage: fc-task <task_struct *>
(e.g. the owner of a VMA reported by fc-faults: fc-task $_fc_task)
end

# fc-curr [cpu]
#   Current task on vCPU <cpu> (default 0). "current" is the per-CPU pointer
#   current_task, normally reached via the GS base: current = *(task **)(gs_base +
#   &current_task). Firecracker's gdb stub does NOT expose $gs_base (it reports only
#   GPRs/rip/eflags), so we resolve the per-CPU base from __per_cpu_offset[cpu] instead
#   — in kernel context gs_base == __per_cpu_offset[smp_processor_id()], and the array
#   needs no live segment register. Pass the cpu matching the gdb thread you are stopped
#   on (`info threads`: Thread 1.<n> is vCPU <n-1>); defaults to 0.
define fc-curr
  if $argc == 0
    set $_cpu = 0
  else
    set $_cpu = $arg0
  end
  set $_pcpu = __per_cpu_offset[$_cpu]
  set $_cur = *(struct task_struct **)((unsigned long)&current_task + $_pcpu)
  fc-task $_cur
end
document fc-curr
Print the current task on vCPU <cpu> (default 0), via __per_cpu_offset[cpu] +
current_task (no $gs_base needed). Usage: fc-curr [cpu]. Linux x86_64 SMP.
end

# fc-regions
#   Print the KASLR-randomized direct-map / vmemmap / vmalloc bases, needed to walk
#   physical<->virtual (__va = phys + page_offset_base) and struct page arrays.
define fc-regions
  printf "page_offset_base = 0x%lx\n", page_offset_base
  printf "vmemmap_base     = 0x%lx\n", vmemmap_base
  printf "vmalloc_base     = 0x%lx\n", vmalloc_base
end
document fc-regions
Print page_offset_base / vmemmap_base / vmalloc_base (for __va and struct page walks).
end

# fc-va <phys>
#   Direct-map virtual address for a guest physical address: phys + page_offset_base.
define fc-va
  printf "__va(0x%lx) = 0x%lx\n", $arg0, ((unsigned long)$arg0 + page_offset_base)
end
document fc-va
Compute the direct-map virtual address of a guest physical address. Usage: fc-va <phys>
end
