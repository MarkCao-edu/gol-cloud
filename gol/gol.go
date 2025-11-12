package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {

	//	TODO: Put the missing channels in here.
	// pgm 图片数据 食材
	// channel数据是做好的饭，只需要加热即可

	ioCommand := make(chan ioCommand)
	//对 startIo 协程下达命令，执行写入pgm图片操作，或者读取 pgm图片操作
	// 或者检查你是否写完了或者读完了
	ioIdle := make(chan bool)
	// 检查写完了或者读完了的信号

	ioFilename := make(chan string)
	// 要读的文件名字
	ioOutput := make(chan uint8)
	// 要写入图片的数据
	ioInput := make(chan uint8)
	// 读的图片数据

	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	go startIo(p, ioChannels)
	//fmt.Println("startIo succeed")

	distributorChannels := distributorChannels{
		events:     events,
		ioCommand:  ioCommand,
		ioIdle:     ioIdle,
		ioFilename: ioFilename,
		ioOutput:   ioOutput,
		ioInput:    ioInput,
	}
	distributor(p, distributorChannels)
}
