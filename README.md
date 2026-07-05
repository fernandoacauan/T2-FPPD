# T2 FPPD - Matrix Multiplication with MPI

> **Authors:** Diogo Giacoboni, Felipe Araújo, Fernando Acauan, Pedro Trarbach

A **static master-slave** parallel implementation of matrix multiplication in Go, using MPI (via [gompi](https://github.com/mvneves/gompi)). The workload is partitioned by rows at startup and distributed across processes with no dynamic re-scheduling, evaluated on the Atlântica cluster across varying node/process configurations.

## Compilation

### Sequential

```bash
cd Sequencial
go build -o sequencial .
```


### Parallel

```
cd Paralelo
go build -o paralelo .
```

## Running

### Sequential

```bash
./sequencial
```

### Parallel (on Atlântica cluster)

```bash
salloc -N <nodes> -n <processes>
mpirun -np <processes> ./paralelo
```

after running, you should run this command:

```bash
exit
```

to deallocate.

### Example
2 nodes / 16 processes:

```bash
salloc -N 2 -n 16
mpirun -np 16 ./paralelo
exit
```
