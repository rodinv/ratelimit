package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const CommandArgsReplacer = "{}"

type cmdTemplate struct {
	name string
	args string
}

type RateLimit struct {
	rate     int
	inFlight int
	command  cmdTemplate

	inputArgChan chan string
}

func NewRateLimit(rate, inFlight int, command cmdTemplate) *RateLimit {
	return &RateLimit{rate: rate,
		inFlight:     inFlight,
		command:      command,
		inputArgChan: make(chan string),
	}
}

func (r *RateLimit) StartReadInput() {
	go func() {
		reader := bufio.NewReader(os.Stdin)

		for {
			argLine, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			r.inputArgChan <- argLine
		}
		close(r.inputArgChan)

	}()
}

func (r *RateLimit) Process() {

	// quota run in second
	rateLimiterChan := make(chan struct{}, r.inFlight)
	for i := 0; i < r.inFlight; i++ {
		rateLimiterChan <- struct{}{}
	}

	go func() {
		for _ = range time.Tick(1000 * time.Millisecond / time.Duration(r.rate)) {
			for i := 0; i < r.inFlight; i++ {
				if len(rateLimiterChan) == cap(rateLimiterChan) { // if limit is full
					break
				}

				rateLimiterChan <- struct{}{}
			}
		}
	}()

	// max goroutines in time
	inflightLimiterChan := make(chan struct{}, r.inFlight)
	for i := 0; i < r.inFlight; i++ {
		inflightLimiterChan <- struct{}{}
	}

	// process input stream
	wg := sync.WaitGroup{}
	for argsIn := range r.inputArgChan {
		wg.Add(1)

		tmpArgLine := strings.ReplaceAll(r.command.args, CommandArgsReplacer, argsIn)
		argsSl := strings.Split(tmpArgLine, " ")

		<-rateLimiterChan
		<-inflightLimiterChan
		go func(cmdName string, args []string) {
			defer func() {
				inflightLimiterChan <- struct{}{}
				wg.Done()
			}()

			comm := exec.Command(cmdName, args...)
			comm.Stdout = os.Stdout
			err := comm.Run()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}(r.command.name, argsSl)
	}
	wg.Wait()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	// check stdin
	if !isFromStdin() {
		return fmt.Errorf("ERROR: input must be from stdin")
	}

	// init args
	argRate := flag.Int("rate", 1, "Maximum number of name starts per second, default 1")
	argInFlight := flag.Int("inflight", 1, "Maximum number of parallel running commands, default 1")
	flag.Parse()

	argCmd, err := getCmd()
	if err != nil {
		return err
	}

	if !(*argRate > 0) {
		return fmt.Errorf("ERROR: rate must be > 0")
	}

	if !(*argInFlight > 0) {
		return fmt.Errorf("ERROR: inflight must be > 0")
	}

	// start processing
	r := NewRateLimit(*argRate, *argInFlight, *argCmd)
	r.StartReadInput()
	r.Process()

	return nil
}

func isFromStdin() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) == 0
}

func getCmd() (*cmdTemplate, error) {
	cmd := new(cmdTemplate)
	if len(flag.Args()) == 0 {
		return nil, fmt.Errorf("ERROR: You must specify the name ")
	}
	cmd.name = flag.Arg(0)

	for i := 1; i < len(flag.Args()); i++ {
		cmd.args += " " + flag.Arg(i)
	}
	cmd.args = strings.TrimPrefix(cmd.args, " ")

	if !strings.Contains(cmd.args, CommandArgsReplacer) {
		return nil, fmt.Errorf("ERROR: You must specify the argument replacer '{}' ")
	}
	return cmd, nil
}
