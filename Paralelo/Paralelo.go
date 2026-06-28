//-----------------------------------------------------------------------------
// File: Paralelo.go
//
// Desc: Implementação da multiplicação paralela do trabalho 2 de FPPD.
//
// Authors: Diogo G., Felipe A., Fernando A. e Pedro T.
//-----------------------------------------------------------------------------
package main;

import (
	"fmt"
	"math/rand"
	"time"

	mpi "github.com/mvneves/gompi"
)

var ( // variaveis globais
	src   = rand.NewSource( 42 );
	rng   = rand.New( src );
	start time.Time;
	comm  = mpi.NewComm( true );
)

const (
	N       = 5000;
	kMestre = 0;
)

//-----------------------------------------------------------------------------
// Name: RowsForRank()
// Desc: Calcula quantas linhas cabem em determinado rank. Se N nao for divisi
//       vel por numProcess, o ultimo processo (rank numProcess - 1) recebe as
//       linhas restantes.
//-----------------------------------------------------------------------------
func RowsForRank( rank int, numProcess int, rowsPerProcess int, mod int ) int {
	rows := rowsPerProcess;

	if rank == numProcess - 1 {
		rows += mod;
	}
	return rows;
}

//-----------------------------------------------------------------------------
// Name: SeedMatrix()
// Desc: Preenche a matriz com valores pseudoaleatorios.
//-----------------------------------------------------------------------------
func SeedMatrix( matrix *[]float64 ) {
	for idx := range *matrix {
		( *matrix )[ idx ] = rng.Float64();
	}
}

//-----------------------------------------------------------------------------
// Name: CalculateMatrix
// Desc: Calcula a multiplicacao das matrizes usando o algoritmo ingenuo.
//-----------------------------------------------------------------------------
func CalculateMatrix( A []float64, B []float64, C *[]float64, rows int) {
	for i := 0; i < rows; i++ {
		for j := 0; j < N; j++ {
			for k := 0; k < N; k++ {
				( *C )[ i * N + j ] += A[ i * N + k ] * B[ k * N + j ];
			}
		}

		if ( i + 1 ) % 500 == 0 {
			fmt.Printf( "  Linha %d/%d concluída (%.1fs)\n", i + 1, rows, time.Since(start).Seconds() );
		}
	}
}

//-----------------------------------------------------------------------------
// Name: SysPrint()
// Desc: Printa o tempo total e a verificacao de canto.
//-----------------------------------------------------------------------------
func SysPrint( matrix []float64, elapsed time.Duration ) {
	fmt.Printf( "\nTempo total: %v\n", elapsed );

	fmt.Printf( "\nVerificação (valores nos cantos da matriz C):\n" );
	fmt.Printf( "  C[0][0]       = %.15f\n", matrix[0] );
	fmt.Printf( "  C[0][N-1]     = %.15f\n", matrix[N-1] );
	fmt.Printf( "  C[N-1][0]     = %.15f\n", matrix[(N-1)*N] );
	fmt.Printf( "  C[N-1][N-1]   = %.15f\n", matrix[(N-1)*N+(N-1)] );
}

//-----------------------------------------------------------------------------
// Name: GetChecksum()
// Desc: Calcula o checksum e printa na tela.
//-----------------------------------------------------------------------------
func GetChecksum( matrix []float64 ) {
	checksum := 0.0;

	for idx := range matrix {
		checksum += matrix[ idx ];
	}

	fmt.Printf( "  Checksum(C)   = %.15f\n", checksum );
}

//-----------------------------------------------------------------------------
// Name: PushMatrixData()
// Desc: Distribui os dados pros processos escravos. Envia a matriz B inteira e
//	 a parte correspondente de A pra cada escravo.
//-----------------------------------------------------------------------------
func PushMatrixData( A []float64, B []float64, numProcess int, rowsPerProcess int, mod int ) {
	// comeca em 1 para skipar a parte do mestre (0 : numElementos)
	for dst := 1; dst < numProcess; dst++ {
		comm.Send( B, dst, 0 )

		rows  := RowsForRank( dst, numProcess, rowsPerProcess, mod );
		start := dst * rowsPerProcess * N;
		end   := start + rows * N;

		comm.Send( A[start:end], dst, 1 );
	}
}

//-----------------------------------------------------------------------------
// Name: PullData()
// Desc: Recebe do mestre a matriz B e o pedaco correspondente da matriz A que o
//       escravo vai calcular.
//-----------------------------------------------------------------------------
func PullData( matrix *[]float64, sliceMatrix *[]float64, rows int ) {
	*matrix      = make( []float64, N * N );
	*sliceMatrix = make( []float64, rows * N );

	comm.Recv( *matrix, kMestre, 0 );
	comm.Recv( *sliceMatrix, kMestre, 1 );
}

//-----------------------------------------------------------------------------
// Name: AssembleMatrix()
// Desc: Coleta todas partes calculadas da matriz C pelos escravos e monta a
//       matriz final.
//-----------------------------------------------------------------------------
func AssembleMatrix( matrix *[]float64, numProcess int, rowsPerProcess int, mod int ) {
	for src := 1; src < numProcess; src++ {
		rows  := RowsForRank( src, numProcess, rowsPerProcess, mod );
		start := src * rowsPerProcess * N;
		end   := start + rows * N;
	
		comm.Recv( (*matrix)[start : end], src, 2 );
	}
}

//-----------------------------------------------------------------------------
// Name: main()
// Desc: O processo mestre gera as matrizes, distribui os dados, calcula a sua
//       parte e coleta os resultados. Os escravos recebem os dados, calculam
//       sua parte e devolvem pro mestre.
//-----------------------------------------------------------------------------
func main() {
	mpi.Init();
	defer mpi.Finalize();

	rank               := comm.GetRank();
	numProcess         := comm.GetSize();
	rowsPerProcess     := N / numProcess;
	mod                := N % numProcess;


	if rank == kMestre {
		A := make( []float64, N*N );
		B := make( []float64, N*N );
		C := make( []float64, N*N );

		SeedMatrix( &A );
		SeedMatrix( &B );

		fmt.Println( "Matrizes geradas." );
		fmt.Printf( "Iniciando multiplicacao (%d x  %d)\n", N, N );

		start = time.Now();

		// o mestre envia B inteiro e um pedaco do A pra cada escravo
		// o ultimo escravo recebe as linhas restantes quando N nao eh divisivel por numProcess
		PushMatrixData( A, B, numProcess, rowsPerProcess, mod );

		// o mestre sempre pega o primeiro pedaco da matriz
		CalculateMatrix( A[0:rowsPerProcess * N], B, &C, rowsPerProcess );

		AssembleMatrix( &C, numProcess, rowsPerProcess, mod );

		elapsed := time.Since( start );

		SysPrint( C, elapsed );
		GetChecksum( C );
		return;
	}

	var B      []float64;
	var sliceA []float64;
	rows       := RowsForRank( rank, numProcess, rowsPerProcess, mod );
	C          := make( []float64, rows * N );

	PullData( &B, &sliceA, rows );

	CalculateMatrix( sliceA, B, &C, rows );

	comm.Send( C, kMestre, 2 ); 
}
