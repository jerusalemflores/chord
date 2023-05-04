package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
)

const (
	defaultHost = "localhost"
	defaultPort = ":3410"
	keySize     = sha1.Size * 8
)

var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)
var allCommands []string

var serverOnline = false

type Feed struct {
	Messages []string
}

type handler func(*Node)
type Server chan<- handler
type Nothing struct{}

type Node struct {
	Address     string
	Indentifier *big.Int
	Successor   []string
	Predecessor string
	Data        map[string]string
}

func initCommands() {
	allCommands = append(allCommands, "Help")
	allCommands = append(allCommands, "Port")
	allCommands = append(allCommands, "Create")
	allCommands = append(allCommands, "Join")
	allCommands = append(allCommands, "Put")
	allCommands = append(allCommands, "Ping")
	allCommands = append(allCommands, "Get")
	allCommands = append(allCommands, "Delete")
	allCommands = append(allCommands, "Dump")
	allCommands = append(allCommands, "Quit")
}

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func startActor(node *Node) Server {

	ch := make(chan handler)
	state := node

	go func() {
		for f := range ch {
			f(state)
		}
	}()
	return ch
}

func main() {
	initCommands()
	fmt.Println("Your current address is: " + getLocalAddress())
	shell(defaultHost + defaultPort)

}

func server(address string, node *Node) {
	actor := startActor(node)
	rpc.Register(actor)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	if err := http.Serve(l, nil); err != nil {
		log.Fatalf("http.Server: %v", err)
	}
}

func client(address string) {

	var trash Nothing
	if err := call(address, "Server.Post", "Hello", &trash); err != nil {
		log.Fatalf("client.Call: %v", err)
	}
	if err := call(address, "Server.Post", "hi", &trash); err != nil {
		log.Fatalf("client.Call: %v", err)
	}

	var lst []string
	if err := call(address, "Server.Get", 5, &lst); err != nil {
		log.Fatalf("client.Call Get: %v", err)
	}

	for _, elt := range lst {
		log.Println(elt)
	}
}

func call(address string, method string, request interface{}, response interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err := client.Call(method, request, response); err != nil {
		log.Printf("client.Call %s: %v", method, err)
		return err
	}
	return nil
}

func shell(address string) {
	log.Printf("Starting interactive shell")
	var node = Node{}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		parts := strings.Split(line, " ")
		if len(parts) > 1 {
			parts[1] = strings.TrimSpace(parts[1])
		}

		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "help":
			if serverOnline == true {
				var trash = Nothing{}
				var commands []string
				if err := call(address, "Server.Help", &trash, &commands); err != nil {
					log.Fatalf("Server calling Server.Help: %v", err)
				}
			}
			for _, elt := range allCommands {
				fmt.Printf(elt + " ")
			}
			fmt.Printf("\n")

		case "port":
			if serverOnline {
				log.Printf("No")
			} else {
				if len(parts) != 2 {
					log.Printf("Invalid command. Try again.\n")
					continue
				}
				address = getLocalAddress() + ":" + parts[1]
				log.Printf("Address: " + address)
			}

		case "quit":
			os.Exit(1)

		case "create":
			var successors = make([]string, 5)
			for i := 0; i < 5; i++ {
				successors = append(successors, address)
			}
			var id = hashString(address)
			var data = make(map[string]string)

			node = Node{Address: address, Indentifier: id, Successor: successors, Predecessor: "", Data: data}
			serverOnline = true
			go server(address, &node)

		case "ping":
			if serverOnline == true {
				if len(parts) != 2 {
					log.Printf("Not specified")
					continue
				}
				var trash = Nothing{}

				if err := call(parts[1], "Server.Ping", &trash, &trash); err != nil {
					continue
				} else {
					log.Printf("success at %v", parts[1])
				}
			} else {
				log.Printf("Turn the server on")
			}
		case "put":
			if serverOnline == true {
				if len(parts) != 4 {
					log.Printf("Review you command line arguments")
					continue
				}
				var trash = Nothing{}

				if err := call(parts[3], "Server.Put", parts[1:3], &trash); err != nil {
					continue
				} else {
					log.Printf("inserted key %v value %v into node at address %v", parts[1], parts[2], node.Address)
				}
			} else {
				log.Printf("Turn the server on")
			}

		case "get":
			if serverOnline == true {
				if len(parts) != 3 {
					log.Printf("Review you command line arguments")
					continue
				}
				var trash = ""
				if err := call(parts[2], "Server.Get", parts[1], &trash); err != nil {
					continue
				} else {
					log.Printf("Retrieved %v", trash)
				}
			} else {
				log.Printf("Turn the server on")
			}

		case "delete":
			if serverOnline == true {
				if len(parts) != 3 {
					log.Printf("only %v out of 3 arguments", len(parts))
					continue
				}
				var trash = Nothing{}
				if err := call(parts[2], "Server.Delete", parts[1], &trash); err != nil {
					continue
				} else {
					log.Printf("Deleted key %v from %v", parts[1], parts[2])
				}
			} else {
				log.Printf("Turn the server on")
			}

		case "dump":
			if serverOnline == true {

				log.Printf("Address %v\nIndentifier: %v", node.Address, node.Indentifier)
				log.Printf("Successors: ")
				for i, s := range node.Successor {
					log.Printf("%v: %v", i, s)
				}
				log.Printf("Predecessor: %v", node.Predecessor)
				for k, v := range node.Data {
					log.Printf(k + ": " + v)
				}
			} else {
				log.Printf("Turn the server on")
			}

		case "join":
			if serverOnline == true {
				log.Printf("already online, can't make a new ring if one already exists")
			} else {
				var successors = make([]string, 5)
				var trash = Nothing{}
				var id = hashString(address)
				var data = make(map[string]string)
				if err := call(parts[1], "Server.Join", node.Address, &trash); err != nil {
					continue
				} else {
					var successor = parts[1]
					for i := 0; i < 5; i++ {
						successors[i] = successor
					}
					log.Printf("Joined a new ring with the node address %v", parts[1])
				}
				node = Node{Address: address, Indentifier: id, Successor: successors, Predecessor: "", Data: data}
				serverOnline = true
				go server(address, &node)
			}
		default:
			log.Printf("Don't reognize")
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("scanner error: %v", err)
	}
}

func (s Server) Help(trash *Nothing, reply *[]string) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		*reply = make([]string, len(allCommands))
		copy(*reply, allCommands)
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Ping(msg *Nothing, reply *Nothing) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Put(kv []string, reply *Nothing) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		finished <- struct{}{}
		n.Data[kv[0]] = kv[1]
	}
	<-finished
	return nil
}

func (s Server) Get(msg string, reply *string) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		finished <- struct{}{}
		if v, exists := n.Data[msg]; exists {
			*reply = v
		}
	}
	<-finished
	return nil
}

func (s Server) Delete(key string, reply *Nothing) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		if _, exists := n.Data[key]; exists {
			delete(n.Data, key)
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Join(addr string, reply *Nothing) error {
	finished := make(chan struct{})
	s <- func(n *Node) {
		finished <- struct{}{}
		n.Predecessor = addr
	}
	<-finished
	return nil
}

func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func jump(address string, fingerentry int) *big.Int {
	n := hashString(address)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}
