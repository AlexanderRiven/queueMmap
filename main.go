package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type queueInfo struct {
	FileN   string `json:"fileN"`   //номер файла для указателя
	StringN int    `json:"stringN"` //порядковый номер символа для указателя
}

func popInfo(f *os.File) (string, int) { //функция получает номер файла и порядковый номер символа для указателя из файлов
	fi, _ := f.Stat()
	buf := bytes.NewBuffer(make([]byte, 0, fi.Size()))
	f.Seek(0, io.SeekStart)
	io.Copy(buf, f)
	line, err := buf.ReadBytes('\n')
	if err != nil && err != io.EOF {
		fmt.Println("error")
	}
	readQ := queueInfo{}
	json.Unmarshal(line, &readQ)
	return readQ.FileN, readQ.StringN

}

func Enqueue(fil *os.File, value string) { //добавляет запись в конец очереди
	filename, stringname := popInfo(fil)
	var f *os.File
	f, err := os.OpenFile(filename+".json", os.O_RDWR, 0644) //открываем файл, имя которого получаем из файла с информацией для указателя
	if errors.Is(err, os.ErrNotExist) {                      //если такого файла еще не существует, создаем и заполняем пустыми байтами
		size := int64(1093)
		f, err := os.Create(filename + ".json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = f.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = f.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = f.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
	}

	defer f.Close()

	mmap, _ := mmap.Map(f, mmap.RDWR, 0) //отображаем файл в память

	defer mmap.Unmap()
	var ptr int
	var ktr int
	var xname int
	xname, _ = strconv.Atoi(filename)
	ptr = stringname

	if stringname+len(value) > len(mmap) { //проверяем, хватает ли места в файле для записи

		//если да, то создаем новый файл, и передаем информацию указателю
		xname, _ = strconv.Atoi(filename)
		f.Close()
		xname += 1
		filename = strconv.Itoa(xname)
		stringname = 0
		ptr = 0
		size := int64(2011)

		f, err := os.Create(filename + ".json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = f.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = f.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = f.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
	}

	for i := 0; i < len(value); i++ { //добавляем запись в файл

		mmap[stringname+i] = value[i]
		ktr = i + 1
	}
	ptr = ptr + ktr
	ktr = 0
	mmap.Flush()
	os.Truncate("writeQueue.json", 0) //обновляем файл для указателя
	newInf := queueInfo{}
	newInf.FileN = strconv.Itoa(xname)
	newInf.StringN = ptr
	newInfM, _ := json.Marshal(newInf)
	err = ioutil.WriteFile("writeQueue.json", newInfM, 0644)
	f.Close()
	mmap.Flush()
	mmap.Unmap()
}

func Dequeue(fil *os.File) (el string) { //функция для получения первой в очереди записи
	filename, stringname := popInfo(fil) //получаем информацию для указателя

	f, _ := os.OpenFile(filename+".json", os.O_RDWR, 0644)

	mmap, _ := mmap.Map(f, mmap.RDWR, 0) //отображаем файл в память
	defer mmap.Unmap()
	strlen, _ := strconv.Atoi(string(mmap[stringname : stringname+2])) //читаем первые два байта, чтобы определить размер записи
	if strlen == 0 {                                                   //если размер 0, то  записей в очереди нет
		fmt.Println("Нет записей в очереди")
	}

	el = string(mmap[stringname+2 : stringname+2+strlen]) //получаем саму запись

	stringname = stringname + 2 + strlen //продвигаем указатель вперед на длину записи
	var xname int
	xname, _ = strconv.Atoi(filename)
	f.Close()
	if (mmap[stringname]) == '\x00' { //если после записи идет пустой байт, то переходим на следующий файл и удаляем предыдущий
		mmap.Unmap()

		e := os.Remove(strconv.Itoa(xname) + ".json")
		if e != nil {
			log.Fatal(e)
		}
		xname, _ = strconv.Atoi(filename)
		xname += 1
		stringname = 0

	}
	mmap.Flush()
	os.Truncate("readQueue.json", 0) //обновляем информацию в файле для указателя
	newInf := queueInfo{}
	newInf.FileN = strconv.Itoa(xname)
	newInf.StringN = stringname
	newInfM, _ := json.Marshal(newInf)
	ioutil.WriteFile("readQueue.json", newInfM, 0644)
	if len(el) == 0 {
		fmt.Println(newInf.FileN)
		fmt.Println(newInf.StringN)
	}
	return el
}

func main() {

	if _, err := os.Stat("0.json"); errors.Is(err, os.ErrNotExist) { //проверяем на наличие первого файла, если его не существует то создаем
		size := int64(2011)
		fd, err := os.Create("0.json")
		if err != nil {
			log.Fatal("Failed to create output")
		}
		_, err = fd.Seek(size-1, 0)
		if err != nil {
			log.Fatal("Failed to seek")
		}
		_, err = fd.Write([]byte{0})
		if err != nil {
			log.Fatal("Write failed")
		}
		err = fd.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
		fd.Close()
	}

	//проверяем на наличие файла с информацией для указателя чтения, если его не существует то создаем
	fileQueueRead, err := os.OpenFile("readQueue.json", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	fi, _ := fileQueueRead.Stat()
	if fi.Size() == 0 {
		newInf := queueInfo{}
		newInf.FileN = "0"
		newInf.StringN = 0
		newInfM, _ := json.Marshal(newInf)
		err = ioutil.WriteFile("readQueue.json", newInfM, 0644)
	}
	defer fileQueueRead.Close()
	//проверяем на наличие файла с информацией для указателя записи, если его не существует то создаем
	fileQueueWrite, err := os.OpenFile("writeQueue.json", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	fi, _ = fileQueueWrite.Stat()
	if fi.Size() == 0 {
		newInf := queueInfo{}
		newInf.FileN = "0"
		newInf.StringN = 0
		newInfM, _ := json.Marshal(newInf)
		err = ioutil.WriteFile("writeQueue.json", newInfM, 0644)
	}
	defer fileQueueWrite.Close()
	var jk string
	_ = jk
	kekmem := "101234567890"
	Enqueue(fileQueueWrite, kekmem)

	for i := 0; i < 200; i++ {

		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)
		Enqueue(fileQueueWrite, kekmem)

		jk = Dequeue(fileQueueRead)
		fmt.Println(len(jk))
		fmt.Println(jk)
		jk = Dequeue(fileQueueRead)
		fmt.Println(jk)
		jk = Dequeue(fileQueueRead)
		fmt.Println(jk)
	}
}
