package main

import (
	"database/sql"
	"log"
	"os"
	"strconv"
	"syscall"
	"time"
	"unsafe"
	"github.com/joho/godotenv"

	_ "github.com/go-sql-driver/mysql"
)

type event_log struct {
	Key          int    `json:"key"`
	Value        string `json:"value"`
	Resource     string `json:"resource"`
	ModifiedTime time.Time `json:"modified_time"`
}

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procCreateMutex = kernel32.NewProc("CreateMutexW")
)

func CreateMutex(name string) (uintptr, error) {
	ret, _, err := procCreateMutex.Call(
		0,
		0,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(name))),
	)
	switch int(err.(syscall.Errno)) {
	case 0:
		return ret, nil
	default:
		return ret, err
	}
}

func main() {
	_, err := CreateMutex("Global\\LogDbToFsTest");

	if err != nil {
		log.Fatal("fatal error, only one instance can run");
		return;
	}

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	loginStr := os.Getenv("DB_LOGIN_STRING");

	db, err := sql.Open("mysql", loginStr);

	if err != nil {
		log.Fatal(err.Error());
	}

	defer db.Close();

	res, err := db.Query("SELECT * FROM event_log");

	if err != nil {
		log.Fatal(err.Error());
		panic("\n");
	}

	var result []event_log;
	var toSave = make(map[string]map[string]string);

	for res.Next() {
		var el event_log;

		err = res.Scan(&el.Key, &el.Value, &el.Resource, &el.ModifiedTime)
		if err != nil {
			log.Fatal(err.Error())
		}
		result = append(result, el);
	}

	for _, v := range result {
		time.Sleep(time.Microsecond * 100) // Don't want to use all the cpu and disk power possible
		str1 := v.ModifiedTime.Format("2006-01-02");
		str2 := v.ModifiedTime.Format("2006-01-02 15:04:05.000000");

		saveStr := str2 + " [" + v.Resource + "] " + v.Value + "\n";

		if _, ok := toSave[str1]; !ok {
			toSave[str1] = map[string]string{};
		}

		strKey := strconv.Itoa(v.Key);

		if _, ok := toSave[str1][strKey]; !ok {
			toSave[str1][strKey] = "";
		}

		toSave[str1][strKey] = toSave[str1][strKey] + saveStr;
	}

	for k,v := range toSave {
		if _, err := os.Stat("./logs/" + k); os.IsNotExist(err) {
			os.Mkdir("./logs/" + k, os.ModePerm);
		}

		for key, val := range v {
			f, err := os.OpenFile("./logs/"+k+"/"+key, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err);
				return;
			}

			if _, err := f.Write([]byte(val)); err != nil {
				log.Fatal(err);
				return;
			}

			if err := f.Close(); err != nil {
				log.Fatal(err);
				return;
			}
			log.Println("Appended data to /logs/" + k + "/" + key);
		}
	}

	db.Exec("DELETE FROM event_log");
	log.Println("All done, inserted " + strconv.Itoa(len(result)) + " rows into fs and deleted them from database");
}