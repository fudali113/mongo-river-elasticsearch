package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"river"
)

func main() {
	var configDir string
	flag.StringVar(&configDir, "config", "config.yml", "配置文件地址")
	config, err := river.InitConfig(configDir)
	checkErr(err)
	// exit
	exit := make(chan string, 1)
	checkErr(river.Run(*config, exit))
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, os.Kill)
		s := <-signals
		exit <- s.String()
	}()
	exitInfo := <-exit
	log.Println("exit: ", exitInfo)
}

func checkErr(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
