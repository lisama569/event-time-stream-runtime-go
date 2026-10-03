package stream

import (
	"math"
	"sort"
)

// deterministicSum 对一组浮点值做与到达顺序无关的求和。
//
// 浮点加法不满足结合律，同一批值按不同顺序累加会在末位产生细微差别。
// 这里先把值按全序（数值，再按比特位打破 -0/+0 之类的平局）排序，
// 使累加顺序只取决于值的多重集合本身，然后用 Neumaier 补偿求和
// 降低舍入误差。因此：同一逻辑输入无论事件如何交错、重复投递后
// 如何去重，窗口 Sum 都逐比特一致，可跨运行直接做字节级对比。
//
// 输入保证为有限值（解析阶段已拒绝 NaN/Inf）。
func deterministicSum(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a != b {
			return a < b
		}
		// 数值相等（如 -0 与 +0）时按比特位定序，保证全序确定。
		return math.Float64bits(a) < math.Float64bits(b)
	})
	// Neumaier（Kahan-Babuska）补偿求和：在固定顺序上进一步压低误差。
	sum := sorted[0]
	comp := 0.0
	for _, v := range sorted[1:] {
		t := sum + v
		if math.Abs(sum) >= math.Abs(v) {
			comp += (sum - t) + v
		} else {
			comp += (v - t) + sum
		}
		sum = t
	}
	return sum + comp
}
