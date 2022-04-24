package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

type Required struct {
	Source      string
	Destination string
}

type Block struct {
	Timestamp int64  // 8 bytes
	ID        int64  // 8 bytes
	Name      string // :gap
	Body      []byte // gap:
}

type Cluster struct {
	Name string
	End  chan bool
}
type Seeker struct {
	Cluster
}
type Timer struct {
	Cluster
}
type Cutter struct {
	Cluster
}
type Sender struct {
	Cluster
}
type Controller struct {
	Cluster
}
type Keeper struct {
	Cluster
}
type Writer struct {
	Cluster
}

var mut sync.Mutex

// timestamp: *[]byte(Block)
var tslink map[int64]*[]byte

// filename: parts number
var fileinfo map[string]int

// block.name: {block.id: *block.body}
var lib map[string]map[int64]*[]byte

const BUFFER = 1e6

func check(err error) {
	if err != nil {
		log.Println(err)
	}
}

func checkerr(msg string, err error) {
	if err != nil {
		log.Printf(msg, err)
	}
}

func findGap(array []byte) (int, int) {
	at := 16
	end := 20
	slice := array[at:end]
	for {
		if string(slice) != "\xFE\xFE\xFE\xFE" {
			at++
			end++
			slice = array[at:end]
		} else {
			break
		}
	}
	return at, end
}

func serialize(block Block) (array []byte) {
	stamp := make([]byte, 8)
	value := make([]byte, 8) // ID
	name := []byte(block.Name)
	gap := []byte{'\xFE', '\xFE', '\xFE', '\xFE'}
	binary.BigEndian.PutUint64(stamp, uint64(block.Timestamp))
	binary.BigEndian.PutUint64(value, uint64(block.ID))
	array = append(array, stamp...)
	array = append(array, value...)
	array = append(array, name...)
	array = append(array, gap...)
	array = append(array, block.Body...)
	return array
}

func deserialize(array []byte) (b Block) {
	if len(array) == 0 {
		return Block{}
	}
	gap, end := findGap(array)
	b.Timestamp = int64(binary.BigEndian.Uint64(array[:8]))
	b.ID = int64(binary.BigEndian.Uint64(array[:16]))
	b.Name = string(array[16:gap])
	b.Body = array[end:]
	return b
}

func showHelp() {
	fmt.Println(`
Usage:
darc-server [-h] source destination

Required args:
source
destination
 `)
}

func Checkout(args []string) (Required, error) {
	var req Required
	for i, arg := range args {
		println(i, arg)
	}
	switch len(args) {
	case 0:
		showHelp()
		os.Exit(0)
	case 1:
		if args[0][0] == '-' {
			if args[0][1] == 'h' {
				showHelp()
				os.Exit(0)
			} else {
				return req, errors.New("wrong command line")
			}
		}
		return req, errors.New("wrong command line")
	case 2:
		if args[0][0] == '-' {
			if args[0][1] == 'h' {
				showHelp()
				os.Exit(0)
			} else {
				return req, errors.New("wrong command line")
			}
		}
		req.Source = args[1]
		req.Destination = args[2]
		return req, nil
	default:
		return req, errors.New("wrong command line")
	}
	return Required{}, nil
}

func (c *Cluster) Timer(b chan []byte, wr chan int64) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-c.End:
			return
		case done := <-wr:
			mut.Lock()
			delete(tslink, done)
			mut.Unlock()
		case <-ticker.C:
			var tskeys []int64
			TS := time.Now().UnixNano() - 1e10
			for k := range tslink {
				if k < TS {
					tskeys = append(tskeys, k)
				}
			}
			for _, tskey := range tskeys {
				var block []byte
				nano := time.Now().UnixNano()
				bytes := *tslink[tskey]
				stamp := make([]byte, 8)
				binary.BigEndian.PutUint64(stamp, uint64(nano))
				block = append(block, stamp...)
				block = append(block, bytes[8:]...)
				bytes = nil
				b <- block
				mut.Lock()
				delete(tslink, tskey)
				tslink[nano] = &block
				mut.Unlock()
			}
		}
	}
}

func (c *Cluster) Seeker(req Required, r chan bool, name chan string, save chan string) {
	ticker := time.NewTicker(1 * time.Second)
	var srcDirEntry []os.DirEntry
	var err error
	for {
		select {
		case <-c.End:
			return
		case <-r:
			for _, entry := range srcDirEntry {
				path := fmt.Sprintf(req.Source + string(os.PathSeparator) + entry.Name())
				try, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
				if err != nil {
					continue
				} else {
					_ = try.Close()
				}
				if _, ok := fileinfo[entry.Name()]; !ok {
					mut.Lock()
					fileinfo[entry.Name()] = 0
					mut.Unlock()
					name <- entry.Name()
					save <- entry.Name()
					break
				}
			}
		case <-ticker.C:
			srcDirEntry, err = os.ReadDir(req.Source)
			check(err)
		}
	}
}

func (c *Cluster) Cutter(req Required, name chan string, b chan []byte) {
	for {
		select {
		case <-c.End:
			return
		case file := <-name:
			path := fmt.Sprintf(req.Source + string(os.PathSeparator) + file)
			target, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
			check(err)
			stat, err := target.Stat()
			check(err)
			mut.Lock()
			fileinfo[file] = int(math.Ceil(float64(stat.Size()) / BUFFER))
			mut.Unlock()
			for i := 0; i < fileinfo[file]; i++ {
				var block Block
				block.Body = make([]byte, BUFFER)
				n, err := target.Read(block.Body)
				if err != nil && err != io.EOF {
					panic(err)
				}
				nano := time.Now().UnixNano()
				block.ID = int64(i)
				block.Name = file
				block.Body = block.Body[:n]
				block.Timestamp = nano
				bytes := serialize(block)
				block = Block{}
				b <- bytes
				mut.Lock()
				tslink[nano] = &bytes
				mut.Unlock()
			}
			err = target.Close()
			check(err)
		}
	}
}

func (c *Cluster) Sender(conn net.Conn, b chan []byte, cmd chan bool) {
	for {
		select {
		case <-c.End:
			return
		case <-cmd:
			_, err := conn.Write(<-b)
			checkerr("Conn write", err)
		}
	}
}

func (c *Cluster) Controller(conn net.Conn, cmd chan bool, rs Sender) {
	for {
		select {
		case <-c.End:
			rs.End <- true
			return
		default:
			syn := make([]byte, 1)
			_, err := conn.Read(syn)
			if err != nil {
				if err == io.EOF {
					rs.End <- true
					return
				} else {
					checkerr("Controller not EOF err", err)
				}
			}
			switch syn[0] {
			case '\xFF':
				cmd <- true
			default:
				log.Println("Not FF in bundle")
			}
		}
	}
}

func (c *Cluster) Keeper(conn net.Conn, wr chan int64, rc Controller) {
	for {
		select {
		case <-c.End:
			rc.End <- true
			return
		default:
			bunch := make([]byte, BUFFER)
			n, err := conn.Read(bunch)
			if err != nil {
				if err == io.EOF {
					rc.End <- true
					return
				} else {
					checkerr("Keeper not EOF err", err)
				}
			}
			bunch = bunch[:n]
			block := deserialize(bunch)
			mut.Lock()
			lib[block.Name][block.ID] = &block.Body
			mut.Unlock()
			wr <- block.Timestamp
		}
	}
}

func (c *Cluster) Writer(req Required, save chan string) {
	var counter int64
	for {
		select {
		case file := <-save:
			counter = 0
			path := fmt.Sprintf(req.Destination + string(os.PathSeparator) + file + ".xz")
			target, err := os.OpenFile(path, os.O_CREATE, os.ModePerm)
			check(err)
			for i := 0; i < fileinfo[file]; i++ {
				if _, ok := lib[file][counter]; ok {
					_, err = target.Write(*lib[file][counter])
					check(err)
					mut.Lock()
					delete(lib[file], counter)
					mut.Unlock()
					counter++
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
			mut.Lock()
			delete(lib, file)
			mut.Unlock()
			_ = target.Close()
		}
	}
}

func main() {

	to, err := net.Listen("tcp", ":27011")
	checkerr("Listen to", err)
	from, err := net.Listen("tcp", ":27012")
	checkerr("Listen from", err)

	//req, err := Checkout(os.Args[1:])
	//check(err)

	req := Required{
		Source:      "",
		Destination: "",
	}

	// cutter-seeker name request
	r := make(chan bool)
	// seeker-cutter name queue
	name := make(chan string)
	// seeker-writer name queue
	save := make(chan string)
	// blocks queue to send
	b := make(chan []byte)
	// writer-timer blocks cache control
	wr := make(chan int64)
	// controller-sender command to send
	cmd := make(chan bool)

	go func() {
		for {
			connTo, err := to.Accept()
			checkerr("Accept to", err)
			connFrom, err := from.Accept()
			checkerr("Accept from", err)
			println("GO FUNC: Two conns set")
			// every client has his own group of goroutines
			// send blocks to client
			rs := Sender{Cluster: Cluster{Name: "Sender", End: make(chan bool, 1)}}
			// receive blocks from client
			rk := Keeper{Cluster: Cluster{Name: "Keeper", End: make(chan bool, 1)}}
			// receive byte from client and sends command to sender to send one block
			rc := Controller{Cluster: Cluster{Name: "Controller", End: make(chan bool, 1)}}
			go rs.Sender(connTo, b, cmd)
			go rk.Keeper(connFrom, wr, rc)
			go rc.Controller(connTo, cmd, rs)
		}
	}()

	rseek := Seeker{Cluster: Cluster{Name: "Seeker", End: make(chan bool, 1)}}
	go rseek.Seeker(req, r, name, save)
	rtim := Timer{Cluster: Cluster{Name: "Timer", End: make(chan bool, 1)}}
	go rtim.Timer(b, wr)
	rcut := Cutter{Cluster: Cluster{Name: "Cutter", End: make(chan bool, 1)}}
	go rcut.Cutter(req, name, b)
	rwr := Writer{Cluster: Cluster{Name: "Writer", End: make(chan bool, 1)}}
	go rwr.Writer(req, save)
}
