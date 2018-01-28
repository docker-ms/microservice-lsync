package util

type StrSlice []string

func (ss StrSlice) Contains(val string) bool {
  if ss == nil {
    return false
  }
  for _, v := range ss {
    if v == val {
      return true
    }
  }
  return false
}


