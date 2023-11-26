package selects

func RNG(offset int, length int) Range {
	if offset < 0 {
		offset = 0
	}
	if length < 1 {
		length = 10
	}
	return Range{
		Offset: offset,
		Length: length,
	}
}

type Range struct {
	Offset int
	Length int
}

func (rng Range) Page() (page Page) {
	page = PG((rng.Offset/rng.Length)+1, rng.Length)
	return
}

func PG(no int, size int) Page {
	if no < 1 {
		no = 1
	}
	if size < 1 {
		size = 10
	}
	return Page{
		No:   no,
		Size: size,
	}
}

type Page struct {
	No   int
	Size int
}

func (p Page) Range() (rng Range) {
	rng = RNG((p.No-1)*p.Size, p.Size)
	return
}
