package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"strings"
	// 导入您的 gol 包。
	// "uk.ac.bris.cs/gameoflife/gol"
)

// --- 这是您的 Controller, Broker, 和 Worker 之间需要传递的数据结构 (示例) ---
// 您需要根据您的 gol.Params 来定义它们

// Controller 发给 Broker 的启动请求
type StartRequest struct {
	ImageName string
	Turns     int
	Threads   int
}

// Broker 发给 Worker 的工作任务
type WorkTask struct {
	Turn       int
	WorldSlice []uint8 // 示例：世界的一部分
	// ... 其他需要的数据 (例如 Halo 区域)
}

// Worker 返回给 Broker 的结果
type WorkResult struct {
	Turn        int
	ResultSlice []uint8 // 示例：计算完成的部分
	// ... 其他结果
}

// --- Broker RPC 服务 ---
type Broker struct {
	// TODO: 在这里添加 Broker 需要的状态
	// 例如： worker 客户端列表, 游戏状态, turns
}

// 这是 Controller 会调用的方法
func (b *Broker) StartGame(req StartRequest, reply *bool) error {
	log.Println("[Broker] 收到 Controller 的 StartGame 请求:", req.ImageName, req.Turns)

	// TODO: 1. 加载图像 (e.g., pgm.Load)

	// TODO: 2. 将世界(world)分割成 N 份 (N = worker 数量)

	// TODO: 3. 在一个循环中 (for t := 0; t < req.Turns; t++)
	//    a. 将 WorkTask (包含 world slice 和 halo) 发送给所有 Worker
	//    b. 等待所有 Worker 返回 WorkResult
	//    c. 聚合(Aggregate)所有 WorkResult 来构建新的世界状态

	log.Println("[Broker] 游戏计算完成。")
	*reply = true
	return nil
}

// --- Worker RPC 服务 ---
type Worker struct {
	// TODO: 在这里添加 Worker 需要的状态
}

// 这是 Broker 会调用的方法
func (w *Worker) ExecuteTurn(task WorkTask, reply *WorkResult) error {
	log.Println("[Worker] 收到 Broker 的 ExecuteTurn 任务, Turn:", task.Turn)

	// TODO: 1. 在这里运行您的 gol 包中的计算逻辑
	// e.g., gol.CalculateNextState(task.WorldSlice, ...)

	// TODO: 2. 将计算结果放入 reply 中
	// reply.ResultSlice = ...

	log.Println("[Worker] Turn", task.Turn, "计算完成。")
	*reply = WorkResult{Turn: task.Turn}
	return nil
}

// --- Main 函数 ---
func main() {
	// === 1. 定义我们需要的“分布式”参数 ===

	// `-type` 决定了程序扮演的角色
	typeFlag := flag.String("type", "", "Mode to run in (controller, broker, worker)")

	// `-port` (Broker 和 Worker 用)
	portFlag := flag.String("port", "8080", "Port to listen on (for broker/worker)")

	// `-broker` (Controller 用)
	brokerFlag := flag.String("broker", "127.0.0.1:8080", "Broker address (for controller to connect to)")

	// `-workers` (Broker 用)
	workersFlag := flag.String("workers", "127.0.0.1:8081,127.0.0.1:8082", "Comma-separated list of worker addresses (for broker to connect to)")

	// `-image`, `-turns`, `-threads` (Controller 用)
	imageFlag := flag.String("image", "images/512x512.pgm", "Image file (for controller)")
	turnsFlag := flag.Int("turns", 10, "Number of turns (for controller)")
	threadsFlag := flag.Int("threads", 16, "Number of threads (for controller, if needed)") // 您可能不需要这个

	flag.Parse()

	// === 2. 根据 -type 运行不同的逻辑 ===

	switch *typeFlag {
	case "controller":
		log.Println("--- 启动为 Controller ---")
		runController(*brokerFlag, *imageFlag, *turnsFlag, *threadsFlag)

	case "broker":
		log.Println("--- 启动为 Broker ---")
		runBroker(*portFlag, *workersFlag)

	case "worker":
		log.Println("--- 启动为 Worker ---")
		runWorker(*portFlag)

	case "":
		log.Fatal("错误: 必须指定 -type 参数 (controller, broker, 或 worker)")

	default:
		log.Fatalf("错误: 未知的 -type: %s", *typeFlag)
	}
}

// === 3. 实现三个角色的逻辑 ===

func runController(brokerAddr, imagePath string, turns, threads int) {
	log.Println("连接到 Broker:", brokerAddr)

	// TODO: 1. 使用 RPC 连接到 Broker
	client, err := rpc.Dial("tcp", brokerAddr)
	if err != nil {
		log.Fatal("Controller 无法连接到 Broker:", err)
	}
	defer client.Close()

	// TODO: 2. 准备请求
	req := StartRequest{
		ImageName: imagePath,
		Turns:     turns,
		Threads:   threads,
	}
	var reply bool

	// TODO: 3. 调用 Broker 的方法
	log.Println("向 Broker 发送 StartGame 请求...")
	err = client.Call("Broker.StartGame", req, &reply)
	if err != nil {
		log.Fatal("RPC 调用失败:", err)
	}

	if reply {
		log.Println("Broker 报告：游戏成功完成！")
	} else {
		log.Println("Broker 报告：游戏失败。")
	}

	// TODO: 4. 您课程要求的 SDL 键盘监听 (keyPresses) 逻辑也可以放在这里
}

func runBroker(port string, workerAddrs string) {
	log.Println("Broker 正在监听端口:", port)

	// TODO: 1. 注册 Broker RPC 服务
	broker := new(Broker)
	rpc.Register(broker)

	// TODO: 2. 监听 TCP 端口 (等待 Controller 连接)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Broker 监听失败:", err)
	}
	defer listener.Close()

	// TODO: 3. 连接到所有 Worker
	workerClients := make([]*rpc.Client, 0)
	workerList := strings.Split(workerAddrs, ",")
	for _, addr := range workerList {
		log.Println("Broker 正在连接到 Worker:", addr)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Println("警告: Broker 无法连接到 Worker", addr, err)
			// 在真实项目中，这里需要错误处理
		} else {
			workerClients = append(workerClients, client)
		}
	}
	log.Printf("Broker 成功连接到 %d 个 Worker", len(workerClients))
	// 您需要将 workerClients 存储在 broker 结构体中，以便 StartGame 方法可以使用它们

	// TODO: 4. 接受 Controller 的连接
	// (这会阻塞，直到 Controller 连接并调用 StartGame)
	rpc.Accept(listener)
}

func runWorker(port string) {
	log.Println("Worker 正在监听端口:", port)

	// TODO: 1. 注册 Worker RPC 服务
	worker := new(Worker)
	rpc.Register(worker)

	// TODO: 2. 监听 TCP 端口 (等待 Broker 连接)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Worker 监听失败:", err)
	}
	defer listener.Close()

	// TODO: 3. 接受 Broker 的连接
	// (这会阻塞，直到 Broker 连接并调用 ExecuteTurn)
	rpc.Accept(listener)
}
