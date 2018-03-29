package main

import (
    "fmt"
    "net"
    "os"
    "time"
    "runtime"
    "bufio"
    "sync"
    "strings"
)

var mutex = &sync.Mutex{}
var candidateTimer bool
var memberCount int
var state string
var voteCount int
var voteTo string
var heartBeat bool
var leader string
var heartBeatTrack bool
var leaderDead string
/*
	Candidate Timer
*/

func CandidateTimer() {

	time.Sleep(20 * time.Second)

		if candidateTimer !=true {
			mutex.Lock()
			candidateTimer=true
			fmt.Println("CandidatTimer Expired")
			if voteTo == "" {
				// Promote slef for candidate Position
				voteTo ="5004"
				fmt.Println("PromotedSelf for candidate Position")
			}
			mutex.Unlock()
		}

}


/*
	Process Received Message
*/
func ProcessMessage(message string) string{

		mutex.Lock()
		if strings.Contains(message,"RequestVote 5001") {
				if(voteTo == "") {
					voteTo="5001"
					message = "Vote"
					state = "follower"
					fmt.Println("Casted voted for 5001")
				} else {
					message = "NoVote"
					fmt.Println("Request 5001 denied,as I have already voted for",voteTo)
				}
		} else if strings.Contains(message,"RequestVote 5002") {
				if(voteTo == "") {
					voteTo="5002"
					message = "Vote"
					state = "follower"
					fmt.Println("Casted voted for 5002")
				} else {
					message = "NoVote"
					fmt.Println("Request 5002 denied,as I have already voted for",voteTo)
				}
		} else if strings.Contains(message,"RequestVote 5003") {
				if(voteTo == "") {
					voteTo="5003"
					message = "Vote"
					state = "follower"
					fmt.Println("Casted voted for 5003")
				} else {
					message = "NoVote"
					fmt.Println("Request 5003 denied,as I have already voted for",voteTo)
				}
		} else if strings.Contains(message,"RequestVote 5000") {
				if(voteTo == "") {
					voteTo="5000"
					message = "Vote"
					state = "follower"
					fmt.Println("Casted voted for 5000")
				} else {
					message = "NoVote"
					fmt.Println("Request 5000 denied,as I have already voted for",voteTo)
				}
		} else if strings.Contains(message,"Leader 5000") && leader == ""{

			leader="5000"
			message="Ack Leader"
		} else if strings.Contains(message,"Leader 5001") && leader == ""{

			leader="5001"
			message="Ack Leader"
		} else if strings.Contains(message,"Leader 5002") && leader == ""{

			leader="5002"
			message="Ack Leader"
		} else if strings.Contains(message,"Leader 5003") && leader == ""{

			leader="5003"
			message="Ack Leader"
		} else if strings.Contains(message,"HeartBeat") && leader !="5004"{

			message="HeartBeatResponse" 
		} 
		mutex.Unlock()
		return message
}

/*
	Monitor HeartBeat
*/

func MonitorHeartBeat(conn net.Conn) {
        timeoutDuration :=100 * time.Second
        conn.SetReadDeadline(time.Now().Add(timeoutDuration))
        var count int
        for {
            time.Sleep(time.Second * 2)
            message, _ := bufio.NewReader(conn).ReadString('\n')
            if strings.Contains(message,"HeartBeat") {
                if heartBeatTrack !=true {
                    mutex.Lock()
                    heartBeatTrack = true
                    mutex.Unlock()
                }
                message ="HeartBeatResponse"
                conn.Write([]byte(message + "\n"))
                fmt.Println("Heart Beat Response Sent")
            } else if message == "" {
                if count == 3 {

                    mutex.Lock()
						leaderDead = leader
                        state = ""
                        leader = ""
                        voteTo = ""
						heartBeat= false
						candidateTimer=false
						defer mutex.Unlock()
                      	conn.Close()
						break
                }
                fmt.Println("Heart Beat Not Received")
                count++

            } else {

                    break
            }

        }
        return
}


/*
	Receive and Serve Messages 
	for connected Client to this terminal
*/
func ReceiveAndServe(conn net.Conn) {

	var message string
	for {

		message, _ = bufio.NewReader(conn).ReadString('\n')
		message1 := message
		message1 =ProcessMessage(message1)
		conn.Write([]byte(message1 + "\n"))
		if  strings.Contains(message,"HeartBeat") && leader != ""{

				fmt.Println("Leader Elected =",leader)
				MonitorHeartBeat(conn)
		}

	}
}

/*
	SendHearBeat Messages to followers
*/
func SendHeartBeat (conn net.Conn, ports string) {

		for {
			message :="HeartBeat"
			conn.Write([]byte(message + "\n"))
			message, _ = bufio.NewReader(conn).ReadString('\n')
			if strings.Contains(message,"HeartBeatResponse") {

				fmt.Println("Received Heart Beat Response From ",ports)
			} else {

				fmt.Println("Missed an Heart Beat Response From",ports)
			}
		}
}

/*
	BroadCastLeaderMessage
*/
func SendLeaderMessage(conn net.Conn,ports string) {

	message :="Leader 5004"
	conn.Write([]byte(message + "\n"))
	fmt.Println("Leader Update Message Sent to",ports)
	message,_ =bufio.NewReader(conn).ReadString('\n')
    if strings.Contains(message,"Ack Leader") {
        fmt.Println("Successfully updated Leader information by",ports)
    } else {
        fmt.Println("Not Received Leader Ack from",ports)
    } 
}

/* 
	Request For Votes from Peers
*/
func RequestForVote(conn net.Conn,ports string) {

	message :="RequestVote 5004"
	_,err :=conn.Write([]byte(message + "\n"))
	fmt.Println("Requested Vote from ",ports)
	message,_ =bufio.NewReader(conn).ReadString('\n')
	    if err != nil {
        fmt.Println("error from port =",ports)
    }

	mutex.Lock()
	if strings.Contains(message,"Vote") {
		voteCount++
		fmt.Println("CurrentVoteCount =",voteCount)
		fmt.Println("Vote Successfully Received from",ports)
	}
	mutex.Unlock()
	time.Sleep(time.Second * 10)
	if voteCount >= memberCount-1 && voteTo != "5004"{
		mutex.Lock()
		fmt.Println("ReceivedMajority Vote")
		fmt.Println("Will communicate with Peers")
		state="leader"
		if(heartBeat == false) {

			heartBeat = true
		}
		mutex.Unlock()
		SendLeaderMessage(conn,ports)
	} else if voteCount >= memberCount-1 && leaderDead != "" && voteTo == "5004"{

        mutex.Lock()
        fmt.Println("ReceivedMajority Vote")
        fmt.Println("Will communicate with Peers")
        state="leader"
        if(heartBeat == false) {

            heartBeat = true
        }
        mutex.Unlock()
        SendLeaderMessage(conn,ports)
    } else  {
        fmt.Println("Didn't Receive Majority Vote, voteCount = ",voteCount)
		state="follower"
	}
}

/*
	Listen on Connected and ports and repond back
*/
func ListenAndServe (conn net.Conn, port string) {
    /*
       Increment Member Count And wait till Network is setup
     */
    mutex.Lock()
        memberCount++
        mutex.Unlock()
        for {
            if memberCount != 4 {

            } else {
                break
            }
        }
        fmt.Println("All Members registered to network,Candidate Timer Begins")
        go CandidateTimer()
        ReceiveAndServe(conn)
}

/*
	Check and Communicate with Peers when Needed
*/
func CommunicateWithPeers(conn net.Conn,ports string) {

   for {
        if candidateTimer == true {

            if voteTo == "" || voteTo == "5004" {

                RequestForVote(conn,ports)
				break
            }  else if leaderDead != "" && voteTo == ""{

                RequestForVote(conn,ports)
            	break
			} else {

                time.Sleep(5 * time.Second)
                fmt.Println("Cant Request For Vote as I'm",state)
				break
            }
        } 
    }
    mutex.Lock()
        leaderDead =""
        mutex.Unlock()
        if heartBeat == true {

            for {
                SendHeartBeat(conn, ports)
            }
        } else {
            fmt.Println("Monitoring HeartBeatFrom ",voteTo)
        }

    for {
        if leaderDead != "" {
            if leaderDead == ports {
				mutex.Lock()
                memberCount--
				mutex.Unlock()
				fmt.Println("link to old leader closed")
                conn.Close()
                    break
            } else {
				fmt.Println("new one")
				if voteTo == "" || voteTo == "5004"{
                	CandidateTimer()
					RequestForVote(conn,ports)
				}
                    break
            }

        }

    }
        if heartBeat == true {

            for {
                SendHeartBeat(conn, ports)
            }
        } else {
            fmt.Println("Monitoring HeartBeatFrom ",voteTo)
        }
}

/*
	SetupListen Port
*/
func SetupListenPort(port string) {

		 service := ":"+port

			 tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
			 if err!= nil {
				 fmt.Println("Resovle failed ",err.Error())
					 os.Exit(1)
			 }
		 listener, err := net.ListenTCP("tcp", tcpAddr)
			 if err != nil {
				 // Handle error
				 fmt.Println(err.Error())
			 }

		 for {
				 conn, err := listener.Accept()
				 if err != nil {
					 // Handle error
					 fmt.Println(err.Error())
				 }
			 go ListenAndServe(conn,port)
		 }
}

/*
	Dail Ports
*/
func DailPorts(address string, ports string, port string,) {
    var dial net.Conn
        for {
            servAddr := address+":"+ports
              tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
              if err != nil {
                  fmt.Println("ResolveTCPAddr failed:", err.Error())
                      os.Exit(1)
              }
            time.Sleep(1000 * time.Millisecond)

              dial, err = net.DialTCP("tcp", nil, tcpAddr)

              if err != nil {
                  //fmt.Println("Dial failed:", err.Error())
                  //os.Exit(1)
              } else {

                  break
              }
        }
        CommunicateWithPeers(dial,ports)
        fmt.Println("Connection successful to ",address,ports, " by ",port)
        //fmt.Println("getting out of peer")


}


/*
	Setup the server
*/
func CreateServer(ports [5]string, address string,) {

        go SetupListenPort(ports[0])
			for i :=1;i<5;i++ {
				go DailPorts(address,ports[i],ports[0])
			}
}



func main () {
    var address string
    var ports [5]string
	candidateTimer = false
	heartBeatTrack = false
	runtime.GOMAXPROCS(100000000000) 
	ports[0]="5004"
	ports[1]="5000"
	ports[2]="5001"
	ports[3]="5002"
	ports[4]="5003"
    CreateServer(ports,address)


	var input string
	fmt.Scanln(&input)
	//Election(address)
}
