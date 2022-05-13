package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strings"
	_ "strings"
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

var lib map[string]map[int64]*[]byte

// TSLink
// timestamp: *[]byte(Block)
var TSLink map[int64]*[]byte

// filename: parts number
var fileinfo map[string]int

const BUFFER = 512000
const NAMESIZE = 64

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

// Serialize
// [Body BUFFER size, timestamp 8, ID 8, Name 64]
func Serialize(block Block) (array []byte, err error) {
	stamp := make([]byte, 8)
	ID := make([]byte, 8)
	name := make([]byte, NAMESIZE-len(block.Name), NAMESIZE)
	name = append(name, []byte(block.Name)...)
	binary.BigEndian.PutUint64(stamp, uint64(block.Timestamp))
	binary.BigEndian.PutUint64(ID, uint64(block.ID))
	array = append(array, block.Body...)
	array = append(array, stamp...)
	array = append(array, ID...)
	array = append(array, name...)
	return array, nil
}

func Deserialize(array []byte) (b Block, err error) {
	if len(array) < (NAMESIZE + 16) {
		return Block{}, errors.New("wrong block format")
	}
	gap := len(array) - (NAMESIZE + 16)
	b.Body = array[:gap]
	b.Timestamp = int64(binary.BigEndian.Uint64(array[gap : gap+8]))
	b.ID = int64(binary.BigEndian.Uint64(array[gap+8 : gap+16]))
	b.Name = string(bytes.Trim(array[gap+16:], "\x00"))
	return b, nil
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

// Timer
// [Body BUFFER size, timestamp 8, ID 8, Name 64]
func (c *Cluster) Timer(b chan []byte, wr chan int64) {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-c.End:
			return
		case done := <-wr:
			mut.Lock()
			delete(TSLink, done)
			mut.Unlock()
		case <-ticker.C:
			var tskeys []int64
			TS := time.Now().UnixNano() - 1e10
			for k := range TSLink {
				if k < TS {
					tskeys = append(tskeys, k)
				}
			}
			// Block reconstruction with new TS
			for _, tskey := range tskeys {
				var block []byte
				nano := time.Now().UnixNano()
				buff := *TSLink[tskey]
				stamp := make([]byte, 8)
				binary.BigEndian.PutUint64(stamp, uint64(nano))
				gap := len(buff) - (NAMESIZE + 16)
				block = append(block, buff[:gap]...)
				block = append(block, stamp...)
				block = append(block, buff[gap+8:]...)
				buff = nil
				b <- block
				mut.Lock()
				delete(TSLink, tskey)
				TSLink[nano] = &block
				mut.Unlock()
			}
		}
	}
}

// Checking for size and mod time every second
func checkSizeMod(path string) (n int64, u int64, err error) {
	var m time.Time
	file, err := os.OpenFile(path, os.O_WRONLY, os.ModePerm)
	if err != nil {
		return 0, 0, err
	} else {
		st, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return 0, 0, err
		}
		n = st.Size()
		m = st.ModTime()
		_ = file.Close()
	}
	return n, m.UnixNano(), nil
}

// Waiting for file completion
func waitForComplete(path string, r chan bool) {
	var size int64
	var nano int64
	var err error
	size, nano, err = checkSizeMod(path)
	if err != nil {
		return
	}
	time.Sleep(1 * time.Second)
	for {
		num, mod, err := checkSizeMod(path)
		if err != nil {
			return
		}
		if size != num || nano != mod {
			size = num
			nano = mod
			time.Sleep(1 * time.Second)
		} else {
			r <- true
			return
		}
	}
}

//func (c *Cluster) Archivarius(arch chan string, freq chan string, fresp chan *string) {
//	var names []*string
//	for {
//		select {
//		case <-c.End:
//			return
//		case n := <-arch:
//			names = append(names, &n)
//		case file := <-freq:
//			for _, v := range names {
//				if strings.Compare(*v, file) == 0 {
//					fresp <- v
//					break
//				}
//			}
//		}
//	}
//}

// Seeker Seeking and sending new file name to Cutter
func (c *Cluster) Seeker(req Required, r chan bool, name chan string, save chan string) {
	ticker := time.NewTicker(2 * time.Second)
	var srcDirEntry []os.DirEntry
	srcDirEntry, _ = os.ReadDir(req.Source)
	for {
		select {
		case <-c.End:
			return
		case <-r:
			for _, entry := range srcDirEntry {
				if _, ok := fileinfo[entry.Name()]; !ok {
					path := fmt.Sprintf(req.Source + string(os.PathSeparator) + entry.Name())
					size1, mod1, err := checkSizeMod(path)
					if err != nil {
						continue
					}
					time.Sleep(1 * time.Second)
					size2, mod2, err := checkSizeMod(path)
					if err != nil {
						continue
					}
					// If the file is copying right now
					// we are going to check and wait
					if size1 != size2 || mod1 != mod2 {
						go waitForComplete(path, r)
						continue
					}
					mut.Lock()
					fileinfo[entry.Name()] = 0
					lib[entry.Name()] = make(map[int64]*[]byte)
					mut.Unlock()
					name <- entry.Name()
					save <- entry.Name()
					break
				}
			}
		case <-ticker.C:
			srcDirEntry, _ = os.ReadDir(req.Source)
		}
	}
}

// Cutter Splitting the file to BUFFER size blocks
func (c *Cluster) Cutter(req Required, name chan string, b chan []byte, r chan bool) {
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
				array, _ := Serialize(block)
				block = Block{}
				b <- array
				mut.Lock()
				TSLink[nano] = &array
				mut.Unlock()
			}
			err = target.Close()
			check(err)
		default:
			r <- true
			file := <-name
			name <- file
		}
	}
}

//func (c *Cluster) Sender(conn net.Conn, b chan []byte, cmd chan bool) {
//	for {
//		select {
//		case <-c.End:
//			println("SENDER is OFF")
//			return
//		case <-cmd:
//			array := <-b
//			md := fmt.Sprintf("%x", md5.Sum(array))
//			_, err := conn.Write([]byte(md))
//			time.Sleep(100 * time.Millisecond)
//			_, err = conn.Write(array)
//			fmt.Println("SENDER: Bytes sent to client", len(array), "length")
//			checkerr("Conn write", err)
//		}
//	}
//}

// Sender Sending the blocks to the clients with md5 and size check
func (c *Cluster) Sender(conn net.Conn, b chan []byte) {
	for {
		select {
		case <-c.End:
			return
		case array := <-b:
			md16 := md5.Sum(array)
			md := md16[:]

			mdbuf := make([]byte, 0, 20)
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(len(array)))
			mdbuf = append(mdbuf, md...)
			mdbuf = append(mdbuf, length...)

			for {
				syn := make([]byte, 1)
				_, err := conn.Read(syn)
				if err != nil {
					if err == io.EOF {
						return
					} else {
						checkerr("Controller not EOF err", err)
					}
				}
				// FF byte - client ready to get md5+size
				// AC byte - ready to get the block
				switch syn[0] {
				case '\xFF':
					_, _ = conn.Write(mdbuf)
				case '\xAC':
					_, err = conn.Write(array)
					goto F
				default:
					log.Println("Not FF in bundle")
				}
			}
		F:
		}
	}
}

//func (c *Cluster) Controller(conn net.Conn, cmd chan bool, rs Sender) {
//	for {
//		select {
//		case <-c.End:
//			rs.End <- true
//			println("CONTROLLER is OFF")
//			return
//		default:
//			syn := make([]byte, 1)
//			_, err := conn.Read(syn)
//			if err != nil {
//				if err == io.EOF {
//					rs.End <- true
//					println("CONTROLLER is OFF")
//					return
//				} else {
//					checkerr("Controller not EOF err", err)
//				}
//			}
//			switch syn[0] {
//			case '\xFF':
//				println("CONTROLLER found FF byte")
//				cmd <- true
//				println("CONTROLLER sent true to cmd chan")
//			default:
//				log.Println("Not FF in bundle")
//			}
//		}
//	}
//}

// Keeper Receiving archived blocks from the client
func (c *Cluster) Keeper(conn net.Conn, wr chan int64) {
	for {
		select {
		case <-c.End:
			//rc.End <- true
			return
		default:
			bunch := make([]byte, BUFFER+(NAMESIZE+16))
			mdbuf := make([]byte, 20)
			// Receiving 20 bytes of md5+size
			_, err := conn.Read(mdbuf)
			md := mdbuf[:16]
			length := int32(binary.BigEndian.Uint32(mdbuf[16:]))
			for {
				n, err := conn.Read(bunch)
				if err != nil {
					if err == io.EOF {
						//rc.End <- true
						return
					} else {
						checkerr("Keeper not EOF err", err)
					}
				}
				bunch = bunch[:n]
				// If n not equal to size - resend
				if length != int32(n) {
					_, _ = conn.Write([]byte("\xFF"))
					bunch = nil
				// If sizes are equal, but md5s are different
				// Resend
				} else {
					md16 := md5.Sum(bunch)
					mdbunch := md16[:]
					if strings.Compare(string(md), string(mdbunch)) != 0 {
						_, _ = conn.Write([]byte("\xFF"))
						bunch = nil
					// If all OK - bytes acknowledged
					} else {
						_, _ = conn.Write([]byte("\xAC"))
						goto K
					}
				}
			}
		K:
			block, err := Deserialize(bunch)
			check(err)
			//freq <- block.Name
			//filename := <-fresp
			filename := block.Name
			for {
				if _, ok := lib[filename]; !ok {
					time.Sleep(100 * time.Millisecond)
				} else {
					break
				}
			}
			mut.Lock()
			lib[filename][block.ID] = &block.Body
			mut.Unlock()
			wr <- block.Timestamp
		}
	}
}

// Writer Saves archived blocks to archive one by one
func (c *Cluster) Writer(req Required, save chan string) {
	for {
		select {
		case file := <-save:
			path := fmt.Sprintf(req.Destination + string(os.PathSeparator) + file + ".xz")
			target, err := os.Create(path)
			check(err)
			for {
				if fileinfo[file] == 0 {
					time.Sleep(100 * time.Millisecond)
				} else {
					break
				}
			}
			for i := 0; i < fileinfo[file]; {
				if _, ok := lib[file][int64(i)]; ok {
					_, err = target.Write(*lib[file][int64(i)])
					check(err)
					mut.Lock()
					delete(lib[file], int64(i))
					mut.Unlock()
					i++
				} else {
					time.Sleep(500 * time.Millisecond)
				}
			}
			//mut.Lock()
			//delete(lib, file)
			//mut.Unlock()
			_ = target.Close()
		}
	}
}

func main() {

	to, err := net.Listen("tcp", ":27011")
	checkerr("Listen to", err)
	from, err := net.Listen("tcp", ":27012")
	checkerr("Listen from", err)

	//block.name: {block.id: *block.body}
	//var lib map[string]map[int64]*[]byte

	// timestamp: *[]byte(Block)
	TSLink = make(map[int64]*[]byte)

	// filename: parts number
	fileinfo = make(map[string]int)

	// block.name: {block.id: *block.body}
	lib = make(map[string]map[int64]*[]byte)

	//req, err := Checkout(os.Args[1:])
	//check(err)

	req := Required{
		Source:      "/ram/1",
		Destination: "/ram/2",
	}

	// R no buffer
	// cutter-seeker name request
	r := make(chan bool)
	// NAME must have buffer
	// seeker-cutter name queue
	name := make(chan string, 1)
	// seeker-writer name queue
	save := make(chan string, 1)
	// B no buffer
	// blocks queue to send
	b := make(chan []byte)
	// WR may have buffer
	// writer-timer blocks cache control
	wr := make(chan int64)
	// controller-sender command to send
	//cmd := make(chan bool)

	//arch := make(chan string)
	//freq := make(chan string)
	//fresp := make(chan *string)

	//rarc := Archivarius{Cluster: Cluster{Name: "Archivarius", End: make(chan bool, 1)}}
	//go rarc.Archivarius(arch, freq, fresp)

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
			//rc := Controller{Cluster: Cluster{Name: "Controller", End: make(chan bool, 1)}}
			go rs.Sender(connTo, b)
			go rk.Keeper(connFrom, wr)
			//go rc.Controller(connTo, cmd, rs)
		}
	}()

	rseek := Seeker{Cluster: Cluster{Name: "Seeker", End: make(chan bool, 1)}}
	go rseek.Seeker(req, r, name, save)
	rtim := Timer{Cluster: Cluster{Name: "Timer", End: make(chan bool, 1)}}
	go rtim.Timer(b, wr)
	rcut := Cutter{Cluster: Cluster{Name: "Cutter", End: make(chan bool, 1)}}
	go rcut.Cutter(req, name, b, r)
	rwr := Writer{Cluster: Cluster{Name: "Writer", End: make(chan bool, 1)}}
	go rwr.Writer(req, save)

	forever := make(chan bool)
	println("MAIN: in forever")
	<-forever

}
