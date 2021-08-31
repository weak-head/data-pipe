package processor

import "context"

type converter struct {}

func NewConverter() (*converter, error) {
	return &converter{}, nil
}

func (c *converter) Convert(ctx context.Context, from []byte) (to []byte, err error) {
	return from, nil
}
