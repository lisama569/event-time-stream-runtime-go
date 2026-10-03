package stream

import "math/big"

// exactSum 是顺序无关的精确小数累加器。
//
// 背景：float64 加法不满足结合律，同一批值按不同到达顺序累加，
// 末位可能出现细微差别，破坏“结果与到达顺序无关、可逐字节对比”的保证。
// 这里用 big.Rat 保存精确和（每个 float64 都是二进制有理数，可精确表示），
// 加法精确且满足交换律/结合律，因此任意顺序累加结果逐位一致；
// 仅在输出时一次性舍入到 float64（舍入本身确定）。
type exactSum struct {
	r *big.Rat
}

// newExactSum 返回零值累加器。
func newExactSum() exactSum {
	return exactSum{r: new(big.Rat)}
}

// Add 精确累加一个有限 float64。
func (s *exactSum) Add(v float64) {
	var x big.Rat
	x.SetFloat64(v)
	s.r.Add(s.r, &x)
}

// Float 把精确和舍入到最近的 float64（确定性舍入）。
func (s *exactSum) Float() float64 {
	f, _ := s.r.Float64()
	return f
}

// String 以 "分子/分母" 规范形式导出精确和（用于检查点持久化）。
func (s *exactSum) String() string {
	return s.r.RatString()
}

// setString 从规范形式恢复精确和；ok=false 表示格式非法。
func (s *exactSum) setString(text string) (ok bool) {
	_, ok = s.r.SetString(text)
	return ok
}
