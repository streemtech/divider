package redis

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCalculate(t *testing.T) {
	fmt.Println("Test Calculate")
	expected := 4
	result := Calculate(2)
	if expected != result {
		t.Error("Failed")
	}
}

func benchmarkCalculate(workCount, nodeCount, timeoutListCount int, b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d := &DividerData{
			Worker:   make(map[string]string),
			NodeWork: make(map[string]map[string]bool),
		}
		workList, nodeList, affinities, timeoutList := generateData(workCount, nodeCount, timeoutListCount)
		b.StartTimer()

		d.calculateNewWork(workList, nodeList, affinities, timeoutList)

	}
}

func BenchmarkCalculate10Items1Nodes(b *testing.B)        { benchmarkCalculate(10, 1, 0, b) }
func BenchmarkCalculate100Items1Nodes(b *testing.B)       { benchmarkCalculate(100, 1, 0, b) }
func BenchmarkCalculate1000Items1Nodes(b *testing.B)      { benchmarkCalculate(1000, 1, 0, b) }
func BenchmarkCalculate10000Items1Nodes(b *testing.B)     { benchmarkCalculate(10000, 1, 0, b) }
func BenchmarkCalculate100000Items1Nodes(b *testing.B)    { benchmarkCalculate(100000, 1, 0, b) }
func BenchmarkCalculate1000000Items1Nodes(b *testing.B)   { benchmarkCalculate(1000000, 1, 0, b) }
func BenchmarkCalculate10Items5Nodes(b *testing.B)        { benchmarkCalculate(10, 5, 0, b) }
func BenchmarkCalculate100Items5Nodes(b *testing.B)       { benchmarkCalculate(100, 5, 0, b) }
func BenchmarkCalculate1000Items5Nodes(b *testing.B)      { benchmarkCalculate(1000, 5, 0, b) }
func BenchmarkCalculate10000Items5Nodes(b *testing.B)     { benchmarkCalculate(10000, 5, 0, b) }
func BenchmarkCalculate100000Items5Nodes(b *testing.B)    { benchmarkCalculate(100000, 5, 0, b) }
func BenchmarkCalculate1000000Items5Nodes(b *testing.B)   { benchmarkCalculate(1000000, 5, 0, b) }
func BenchmarkCalculate10Items10Nodes(b *testing.B)       { benchmarkCalculate(10, 10, 0, b) }
func BenchmarkCalculate100Items10Nodes(b *testing.B)      { benchmarkCalculate(100, 10, 0, b) }
func BenchmarkCalculate1000Items10Nodes(b *testing.B)     { benchmarkCalculate(1000, 10, 0, b) }
func BenchmarkCalculate10000Items10Nodes(b *testing.B)    { benchmarkCalculate(10000, 10, 0, b) }
func BenchmarkCalculate100000Items10Nodes(b *testing.B)   { benchmarkCalculate(100000, 10, 0, b) }
func BenchmarkCalculate1000000Items10Nodes(b *testing.B)  { benchmarkCalculate(1000000, 10, 0, b) }
func BenchmarkCalculate10Items25Nodes(b *testing.B)       { benchmarkCalculate(10, 25, 0, b) }
func BenchmarkCalculate100Items25Nodes(b *testing.B)      { benchmarkCalculate(100, 25, 0, b) }
func BenchmarkCalculate1000Items25Nodes(b *testing.B)     { benchmarkCalculate(1000, 25, 0, b) }
func BenchmarkCalculate10000Items25Nodes(b *testing.B)    { benchmarkCalculate(10000, 25, 0, b) }
func BenchmarkCalculate100000Items25Nodes(b *testing.B)   { benchmarkCalculate(100000, 25, 0, b) }
func BenchmarkCalculate1000000Items25Nodes(b *testing.B)  { benchmarkCalculate(1000000, 25, 0, b) }
func BenchmarkCalculate10Items100Nodes(b *testing.B)      { benchmarkCalculate(10, 100, 0, b) }
func BenchmarkCalculate100Items100Nodes(b *testing.B)     { benchmarkCalculate(100, 100, 0, b) }
func BenchmarkCalculate1000Items100Nodes(b *testing.B)    { benchmarkCalculate(1000, 100, 0, b) }
func BenchmarkCalculate10000Items100Nodes(b *testing.B)   { benchmarkCalculate(10000, 100, 0, b) }
func BenchmarkCalculate100000Items100Nodes(b *testing.B)  { benchmarkCalculate(100000, 100, 0, b) }
func BenchmarkCalculate1000000Items100Nodes(b *testing.B) { benchmarkCalculate(1000000, 100, 0, b) }

func generateData(workCount, nodeCount, timeoutListCount int) (workList map[string]string, nodeList, affinities, timeoutList map[string]int64) {
	workList = make(map[string]string)
	nodeList = make(map[string]int64)
	affinities = make(map[string]int64)
	timeoutList = make(map[string]int64)

	for i := 0; i < workCount; i++ {
		data := "data:" + strconv.Itoa(i)
		workList[data] = data
	}

	for i := 0; i < nodeCount; i++ {
		data := uuid.New().String()
		nodeList[data] = 1000
		affinities[data] = time.Now().Unix()

	}

	for i := 0; i < timeoutListCount; i++ {
		data := uuid.New().String()
		timeoutList[data] = time.Now().Unix()
	}

	return workList, nodeList, affinities, timeoutList
}

func TestOther(t *testing.T) {
	fmt.Println("Testing something else")
	fmt.Println("This shouldn't run with -run=calc")
}

func Calculate(x int) (result int) {
	fmt.Printf("%d\n", x)
	result = x + 2
	return result
}
