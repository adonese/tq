----------------------------- MODULE TaskQueue -----------------------------

EXTENDS Naturals, TLC

CONSTANTS MaxRetries, NumWorkers, TaskQueueSize

VARIABLES tasks, processedTasks, shuttingDown, workers

(* --algorithm TaskQueue
variables tasks = <<>>, processedTasks = {}, shuttingDown = FALSE, workers = [i \in 1..NumWorkers |-> FALSE];

define Init == /\ tasks = <<>>
              /\ processedTasks = {}
              /\ shuttingDown = FALSE
              /\ workers = [i \in 1..NumWorkers |-> FALSE]

define AddTask(taskId, retries) ==
  /\ shuttingDown = FALSE
  /\ tasks' = Append(tasks, [id |-> taskId, retries |-> retries])

define ProcessTask ==
  /\ Len(tasks) > 0
  /\ \E worker \in 1..NumWorkers: ~workers[worker]
  /\ LET task == Head(tasks)
     IN /\ tasks' = Tail(tasks)
        /\ IF task.retries > 0 THEN
             /\ tasks' = Append(tasks, [id |-> task.id, retries |-> task.retries - 1])
           ELSE
             /\ processedTasks' = processedTasks \cup {task.id}
        /\ workers' = [i \in 1..NumWorkers |-> IF i = worker THEN TRUE ELSE workers[i]]
        /\ workers[worker] = FALSE

define Shutdown ==
  /\ shuttingDown = TRUE
  /\ \A worker \in 1..NumWorkers: ~workers[worker]

init == Init

next == \/ AddTask(taskId, retries)
        \/ ProcessTask
        \/ Shutdown
)

Spec == init /\ [][next]_vars

Termination == shuttingDown => Len(tasks) = 0 /\ \A worker \in 1..NumWorkers: ~workers[worker]
*)

=============================================================================
