package window

import (
	"sort"
	"strconv"
	"strings"
)

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func sortStrings(s []string) { sort.Strings(s) }

func sortResults(r []Result) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].WindowStart != r[j].WindowStart {
			return r[i].WindowStart < r[j].WindowStart
		}
		return r[i].Key < r[j].Key
	})
}

func splitSeen(s string) (string, int64, bool) {
	idx := strings.LastIndex(s, ":")
	if idx <= 0 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(s[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return s[:idx], n, true
}
