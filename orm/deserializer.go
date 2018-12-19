package orm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// Deserializer is a callback which will take a json RawMessage for processing
type Deserializer func(line json.RawMessage) error

// Deserialize will return a function which will Deserialize in a flexible way the JSON in reader
func Deserialize(r io.Reader, dser Deserializer) error {
	bufreader := bufio.NewReader(r)
	buf, err := bufreader.Peek(1)
	if err != nil && err != io.EOF {
		return err
	}
	if err == io.EOF && len(buf) == 0 {
		return nil
	}
	dec := json.NewDecoder(bufreader)
	dec.UseNumber()
	token := string(buf[0:1])
	running := true
	for running {
		switch token {
		case "[", "{":
			{
				if token == "[" {
					// advance the array token
					dec.Token()
				}
				var line json.RawMessage
				for dec.More() {
					if err := dec.Decode(&line); err != nil {
						return err
					}
					if err := dser(line); err != nil {
						return err
					}
				}
				// consume the last token
				dec.Token()
				// check to see if we have more data in the buffer in case we
				// have concatenated streams together
				running = dec.More()
			}
		default:
			return fmt.Errorf("invalid json, expected either [ or {")
		}
	}
	return nil
}
