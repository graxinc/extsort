package extsort

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type tempWriter struct {
	f        *os.File
	c        compressedWriter
	w        *bufio.Writer
	keepFile bool

	scratch []byte
	offsets []int64
	size    int64
}

func newTempWriter(dir string, compress Compression, keepFile bool) (*tempWriter, error) {
	f, err := newTempFile(dir, "extsort", keepFile)
	if err != nil {
		return nil, err
	}

	c := compress.newWriter(f)
	w := bufio.NewWriterSize(c, 1<<16) // 64k
	return &tempWriter{f: f, c: c, w: w, scratch: make([]byte, binary.MaxVarintLen64), keepFile: keepFile}, nil
}

func (t *tempWriter) ReaderAt() io.ReaderAt {
	return t.f
}

func (t *tempWriter) Encode(ent *entry) error {
	if err := t.encodeSize(ent.keyLen); err != nil {
		return err
	}
	if err := t.encodeSize(ent.ValLen()); err != nil {
		return err
	}
	if _, err := t.Write(ent.data.B); err != nil {
		return err
	}
	return nil
}

func (t *tempWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	t.size += int64(n)
	return n, err
}

func (t *tempWriter) Flush() error {
	if err := t.w.Flush(); err != nil {
		return err
	}
	if err := t.c.Close(); err != nil {
		return err
	}

	pos, err := t.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	t.offsets = append(t.offsets, pos)
	t.c.Reset(t.f)
	t.w.Reset(t.c)

	return nil
}

func (t *tempWriter) Close() (err error) {
	if e := t.c.Close(); e != nil {
		err = e
	}
	if e := closeTempFile(t.f, t.keepFile); e != nil {
		err = e
	}
	return
}

func (t *tempWriter) encodeSize(sz int) error {
	n := binary.PutUvarint(t.scratch, uint64(sz))
	if _, err := t.Write(t.scratch[:n]); err != nil {
		return err
	}
	return nil
}

func (t *tempWriter) Size() int64 {
	return t.size
}

// --------------------------------------------------------------------

type tempReader struct {
	readers       []io.ReadCloser
	sections      []*bufio.Reader
	limitedReader io.LimitedReader // here to avoid allocations
}

func newTempReader(ra io.ReaderAt, offsets []int64, bufSize int, compress Compression) (*tempReader, error) {
	r := &tempReader{
		readers:  make([]io.ReadCloser, 0, len(offsets)),
		sections: make([]*bufio.Reader, 0, len(offsets)),
	}
	slimit := bufSize / (len(offsets) + 1)
	offset := int64(0)
	for _, next := range offsets {
		crd, err := compress.newReader(io.NewSectionReader(ra, offset, next-offset))
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		r.sections = append(r.sections, bufio.NewReaderSize(crd, slimit))
		r.readers = append(r.readers, crd)
		offset = next
	}

	return r, nil
}

func (t *tempReader) NumSections() int {
	return len(t.sections)
}

func (t *tempReader) ReadNext(section int) (*entry, error) {
	r := t.sections[section]
	if r == nil {
		return nil, nil
	}

	ku, err := binary.ReadUvarint(r)
	if err == io.EOF {
		t.sections[section] = nil
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	vu, err := binary.ReadUvarint(r)
	if err == io.EOF {
		t.sections[section] = nil
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	ent := fetchEntry()
	ent.keyLen = int(ku)
	t.limitedReader.R = r
	t.limitedReader.N = int64(ku + vu)
	if _, err := ent.data.ReadFrom(&t.limitedReader); err != nil {
		ent.Release()
		return nil, err
	}
	return ent, nil
}

func (t *tempReader) Close() (err error) {
	for _, crd := range t.readers {
		if e := crd.Close(); e != nil {
			err = e
		}
	}
	return
}
