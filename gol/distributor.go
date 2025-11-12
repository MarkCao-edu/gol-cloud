package gol

import (
	"fmt"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune //根据keyboard_test可知chan类型
}

type worker struct {
	state        [][]uint8 //0-256
	newstate     [][]uint8
	width        int
	height       int
	startY, endY int
}

func updateState(worker worker, x, y int, done chan util.Cell) {
	alivecell := 0 //统计活细胞
	//for循环遍历state棋盘横纵行，准确来讲是传入的cell的上下左右
	for dx := -1; dx <= 1; dx++ {
		for dy := -1; dy <= 1; dy++ {
			if dx == 0 && dy == 0 {
				continue
				//一眼顶针，鉴定为cell本体，跳过
			}
			//通过循环遍历neighbor cell
			// nx := x + dx
			// ny := y + dy
			//重铸边界
			// if neighborX == -1 {
			// 	neighborX = worker.width - 1
			// }
			// if neighborX == worker.width-1 {
			// 	neighborX = 0
			// }
			//加本体防出界，避免-1这种情况
			nx := (x + dx + worker.width) % worker.width   //worker.width = len(state[0]) = 图像宽度
			ny := (y + dy + worker.height) % worker.height // worker.height = len(state) = 图像高度
			//出界当死细胞
			//if nx < 0 || ny < 0 || nx >= len(state[0]) || ny >= len(state[0]) {
			//    continue
			//}

			if worker.state[ny][nx] == 255 {
				alivecell++
			}
		}
	}
	//细胞状态更新判断
	if worker.state[y][x] == 255 { //current status
		if alivecell < 2 || alivecell > 3 {
			done <- util.Cell{X: x, Y: y} //收集需要变动的细胞信息进行传输，后续用于反转状态
			worker.newstate[y][x] = 0
		} else {
			worker.newstate[y][x] = 255
		}
	} else { //给死细胞复活用的
		if alivecell == 3 {
			done <- util.Cell{X: x, Y: y} //后续用flippixel，参考window./go和loop.go
			worker.newstate[y][x] = 255
		} else {
			worker.newstate[y][x] = 0
		}
	}
}

func calculateNewBoard(worker worker, done chan util.Cell, threadRecord chan int) {
	for y := worker.startY; y < worker.endY; y++ { // 从图像索引 startY 遍历到 endY-1,i=y
		for x := 0; x < worker.width; x++ { //j=x
			updateState(worker, x, y, done) //每行每列更新细胞状态和
		}
	}
	threadRecord <- 1 //往channel里打一个ping，用于判断是否协程都完成
}

// 分派任务，并发执行细胞状态更新，启动threads个协程，each thread会纵向分配startY到endY行做更新任务
// func startWork(w worker, threads int, done chan util.Cell, threadRecord chan int) {
// 	rowsPerThread := w.height / threads

// 	for t := 0; t < threads; t++ {
// 		startY := t * rowsPerThread
// 		endY := startY + rowsPerThread

// 		// Last worker takes any leftover rows
// 		if t == threads-1 {
// 			endY = w.height
// 		}
// 		// go work1
// 		// work2
// 		// 同一个 worker
// 		go calculateNewBoard(w, startY, endY, done, threadRecord)
// 	}
// }

//func getAliveCells(state [][]uint8, width, height int) int {
//	Alieve := 0
//	for y := 0; y < height; y++ {
//		for x := 0; x < width; x++ {
//			if state[y][x] == 255 {
//				Alieve++
//			}
//		}
//	}
//	return Alieve
//}

func getAliveCellsOnBoard(state [][]uint8, width, height int) []util.Cell {
	aliveCells := []util.Cell{} //活细胞合集
	//count:=0 //直接len(aliveCells[])可解
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if state[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{Y: y, X: x})
				//count++
			}
		}
	}
	return aliveCells
}

func readInitialState(p Params, d distributorChannels) [][]uint8 {
	d.ioCommand <- ioInput
	// fmt.Println("Input")
	d.ioFilename <- fmt.Sprintf("%vx%v", p.ImageWidth, p.ImageHeight) //根据util_test
	// fmt.Println("ioCommand send succeed")
	state := make([][]uint8, p.ImageHeight)
	for i := range state {
		state[i] = make([]uint8, p.ImageWidth)
	}
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			state[y][x] = <-d.ioInput
		}
	}
	return state
}

//func solve(done chan util.Cell, state [][]uint8, newstate [][]uint8) {}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {

	state := readInitialState(p, c)

	pause := false   // 程序是否暂停，默认false，防爆
	quit := false    //是否退出程序
	saveQuit := true //非quit下的退出保存
	// fmt.Println(state)

	//buffered channel

	threadRecord := make(chan int, p.Threads) //用于记录协程，我记得定义里为8个(?

	//初始化棋盘界面
	alievecells := getAliveCellsOnBoard(state, p.ImageWidth, p.ImageHeight)
	for _, cell := range alievecells {
		c.events <- CellFlipped{CompletedTurns: 0, Cell: cell}
	}

	turn := 0
	c.events <- StateChange{turn, Executing}
	//初始化世界
	ticker := time.NewTicker(2 * time.Second) //创建一个定时器，每 2 秒触发一次，链接ticker.C
	for turn < p.Turns && !quit {
		select { //开始监听which channel is ready
		//键盘输入部分定义
		case key := <-c.keyPresses: // 只有当 c.keyPresses 有数据可读时才会命中这个分支，key为新var，从 c.keyPresses 读出一个值赋给 key，本质receive
			switch key { //开始匹配case
			case 'p':
				if !pause {
					pause = true
					c.events <- StateChange{CompletedTurns: turn, NewState: Paused} //按p，更新状态为“暂停”-true
				} else {
					pause = false
					c.events <- StateChange{CompletedTurns: turn, NewState: Executing} //本来就在暂停，更新状态为“继续执行”-false，根据event.go
				}
			case 's':
				c.ioCommand <- ioOutput                                                    //给IO协程发命令：“开始输出图像”
				c.ioFilename <- fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turn) //根据test要求规范传输的filename
				//0123
				//4567
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						c.ioOutput <- state[y][x]
					}
				}
				//发布检查指令，确保磁盘写入完成
				c.ioCommand <- ioCheckIdle //你现在有空吗
				<-c.ioIdle                 // 我现在有空
				c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turn)}
			case 'q':
				c.ioCommand <- ioOutput
				c.ioFilename <- fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turn)
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						c.ioOutput <- state[y][x]
					}
				}
				c.ioCommand <- ioCheckIdle
				<-c.ioIdle
				c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turn)}
				quit = true      //退出循环
				saveQuit = false //后续正常保存退出不执行
			}

		case <-ticker.C:
			//aliveCount := getAliveCells(state, p.ImageWidth, p.ImageHeight)
			aliveCount := getAliveCellsOnBoard(state, p.ImageWidth, p.ImageHeight)
			c.events <- AliveCellsCount{CompletedTurns: turn, CellsCount: len(aliveCount)} //参考event.go

		default:
			if !pause {
				// fmt.Println("into default")
				done := make(chan util.Cell, p.ImageHeight*p.ImageWidth) //总容量为棋盘pixel总数，用于存储需要“反转”更新的细胞，传递至SDL
				//初始化棋盘
				newState := make([][]uint8, p.ImageHeight) // ImageHeight 列
				for i := 0; i < p.ImageHeight; i++ {       //i=y
					newState[i] = make([]uint8, p.ImageWidth)
				}
				rowsPerThread := p.ImageHeight / p.Threads // 每个线程领取的任务

				for t := 0; t < p.Threads; t++ {
					// startY endY 任务范围
					startY := t * rowsPerThread
					endY := startY + rowsPerThread

					// Last worker takes any leftover rows
					if t == p.Threads-1 {
						endY = p.ImageHeight
					}
					// go work1
					// 初始化一个新的worker，根据协程数量分worker
					go calculateNewBoard(worker{
						startY:   startY,        //切分起点
						endY:     endY,          //切分终点
						newstate: newState,      // 初始化还没有更新的新棋盘
						state:    state,         // 上一轮更新完成的棋盘
						width:    p.ImageWidth,  //图像宽度
						height:   p.ImageHeight, //图像高度
					}, done, threadRecord)
				}
				for i := 0; i < p.Threads; i++ {
					<-threadRecord //数是否所有线程执行完成任务
				}

				close(done)
				alievecells := []util.Cell{}
				for cell := range done { //channel range不需要屏蔽index
					alievecells = append(alievecells, cell)
				}
				c.events <- CellsFlipped{CompletedTurns: turn, Cells: alievecells}
				state = newState
				turn++
				c.events <- TurnComplete{CompletedTurns: turn}
				// fmt.Println("current turn = ", turn)
				// fmt.Println(state)

			} else {
				time.Sleep(10 * time.Millisecond) //让出 CPU
			}

		}
	}

	//初始化棋盘

	finalCells := getAliveCellsOnBoard(state, p.ImageWidth, p.ImageHeight)

	c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: finalCells}

	// TODO: Execute all turns of the Game of Life.

	// TODO: Report the final state using FinalTurnCompleteEvent.

	// Make sure that the Io has finished any output before exiting.
	if saveQuit {
		c.ioCommand <- ioOutput
		c.ioFilename <- fmt.Sprintf("%vx%vx%v", p.ImageWidth, p.ImageHeight, turn)
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				c.ioOutput <- state[y][x]
			}
		}
		c.ioCommand <- ioCheckIdle
		<-c.ioIdle
	}

	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
