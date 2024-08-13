package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"kafkaDemo/config"
	"kafkaDemo/core"
	"kafkaDemo/storage/couchbase"
	"kafkaDemo/storage/kafka"
	"kafkaDemo/storage/logging"
	"kafkaDemo/util/common"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// const topicName = "go_training_hardik"
// const hostURL = "192.168.26.10:9092"

var (
	topic                                     = config.GetString("Kafka.Topics.SourceTopic")
	metricsTopic                              = config.GetString("Kafka.Topics.MetricsTopic")
	consumerGroup                             = config.GetString("Kafka.ConsumerGroup")
	batch                                     = config.GetInt("KafkaBatchSize")
	sourceBucket                              = config.GetString("Couchbase.Buckets.SourceBucket")
	destinationBucket                         = config.GetString("Couchbase.Buckets.DestinationBucket")
	cbConfigDocKey                            = config.GetString("Couchbase.CBConfigDocKey")
	cbConfigBucket                            = config.GetString("Couchbase.CBConfigBucket")
	receiverDomains                           = config.GetString("Receiver.Host")
	upsertEndpoint                            = config.GetString("Receiver.Endpoints.Upsert")
	removeEndpoint                            = config.GetString("Receiver.Endpoints.Remove")
	receiverGlobalConnections                 = make(map[int]core.Receiver)
	receiverDomainsArray                      = strings.Split(receiverDomains, ",")
	kafkaMessageKey                           = config.GetString("Metrics.KafkaMessageKey")
	typeName                                  = config.GetString("Metrics.LabelType")
	batchIntervalTime                         = config.GetInt64("WaitBetweenBatches_MiliSecond")
	keepDockeyPrefix                          = config.GetString("KeepDockeyPrefix")
	skipDockeyPrefix                          = config.GetString("SkipDockeyPrefix")
	sleepTimeBeforeNextCheckForInActiveSender = config.GetInt64("SleepTimeBeforeNextCheckForInActiveSenderInSeconds")
)

func main() {
	fmt.Println("****************************")
	fmt.Println("1) Kafka Message Cousumer")
	fmt.Println("2) Kafka Message Producer")
	fmt.Println("3) Commit Kafka Messages")
	fmt.Println("4) Couchbase Get Documents")
	fmt.Println("5) Couchbase Insert|Upate Documents")
	fmt.Println("6) Couchbase Delete Documents")
	fmt.Println("7) Compress Documents")
	fmt.Println("8) Exit")
	fmt.Println("****************************")
	choice, _ := ScanIntergerValue("Please enter valid choice")
	switch choice {
	case 1:
		CounsumerFunc()
	case 2:
		ProducerFunc()
	case 3:
		CommitOffet()
	case 4:
		GetCouchbaseDocument()
	case 5:
		InsertCouchbaseDocument("demo", "cust-01-", 1000)
		DeleteCouchbaseDocument("demo", "cust-01-", 1000)
		time.Sleep(2 * time.Second)
		InsertCouchbaseDocument("demo", "cust-01-", 2000)
		DeleteCouchbaseDocument("demo", "cust-01-", 2000)
		time.Sleep(2 * time.Second)
		InsertCouchbaseDocument("demo", "cust-01-", 2000)
		DeleteCouchbaseDocument("demo", "cust-01-", 2000)
		time.Sleep(2 * time.Second)
		InsertCouchbaseDocument("demo", "cust-01-", 2000)
		DeleteCouchbaseDocument("demo", "cust-01-", 2000)
		time.Sleep(2 * time.Second)
		InsertCouchbaseDocument("demo", "cust-01-", 3000)
		DeleteCouchbaseDocument("demo", "cust-01-", 3000)
	case 6:
		DeleteCouchbaseDocument("demo", "cust-01-", 1000)
	case 7:
		GzipData()
	case 8:
		os.Exit(1)

	default:
		fmt.Println("Not found..!")
	}
}

func readJSONFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func GzipData() {
	filenames := []string{
		"json/doc.json",
		"json/doc-1.json",
		"json/doc-2.json",
		"json/doc-3.json",
	}

	for _, filename := range filenames {
		// Read JSON file
		data, err := readJSONFile(filename)
		if err != nil {
			log.Fatal(err)
		}

		// Print length of JSON document before gzip
		fmt.Printf("Length of %s before gzip: %d bytes\n", filename, len(data))

		// Compress the data
		compressedData, err := common.CompressDataTOGzip(data)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Length of %s after gzip: %d bytes\n", filename, len(compressedData.Bytes()))
		fmt.Println("===============================================================================")
	}
}

// scan the user integer value and  validate then return
func ScanIntergerValue(errorMessage string) (int, error) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if scanner.Scan() {
			_, err := strconv.Atoi(scanner.Text())
			if err != nil {
				fmt.Println(errorMessage)
			} else {
				break
			}
		}
	}
	return strconv.Atoi(scanner.Text())
}

func CounsumerFunc() {
	consumeMessageList, err_consume := consumeMessage(batch)
	if err_consume != nil {
		fmt.Println(err_consume)
	} else {
		fmt.Println(consumeMessageList)
	}
	errCommitOffset := kafka.CommitOffset(topic, consumerGroup)
	if errCommitOffset != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(errCommitOffset))
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "commit offset successfully on kafka", nil)
	}
	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers": hostURL,
	// 	"group.id":          "hp1",
	// 	"auto.offset.reset": "earliest",
	// })

	// if err != nil {
	// 	panic(err)
	// }

	// c.SubscribeTopics([]string{topicName}, nil)

	// for {
	// 	msg, err := c.ReadMessage(-1)
	// 	if err == nil {
	// 		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	// 	} else {
	// 		// The client will automatically try to recover from all errors.
	// 		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	// 	}
	// }

	// c.Close()
}

func ProducerFunc() {

	// p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": hostURL})
	// if err != nil {
	// 	panic(err)
	// }

	// defer p.Close()

	// go func() {
	// 	for e := range p.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
	// 			} else {
	// 				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
	// 			}
	// 		}
	// 	}
	// }()

	// // Produce messages to topic (asynchronously)
	// topic := topicName
	// for _, word := range []string{"CUST_121212", "CUST_123456", "CUST_232323", "CUST_454545", "CUST_858585"} {
	// 	p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte(word),
	// 	}, nil)
	// }

	// // Wait for message deliveries before shutting down
	// p.Flush(15 * 1000)
}

// consume the message from the kafka and return the customer id list
func consumeMessage(batch int) (KafkaKeyValueList []core.KafkaEventPayload, kafkaError error) {
	result, err_consume := kafka.Consume(topic, consumerGroup, batch)
	if err_consume != nil {
		kafkaError = errors.New(err_consume.Error())
	} else {
		for _, msg := range result.Messages {
			var kafKeyValue core.KafkaEventPayload
			var valueData core.KafkaValue_CBtoKafka_Struct

			valueErr := json.Unmarshal(msg.Value, &valueData)
			if valueErr != nil {
				kafkaError = logging.EnrichErrorWithStackTrace(valueErr)
			} else {
				kafKeyValue.Payload = valueData.Key
				kafKeyValue.Event = valueData.Event
				KafkaKeyValueList = append(KafkaKeyValueList, kafKeyValue)
			}
			fmt.Println(msg)
			//var keyData core.KafkaKey_CBtoKafka
			// keyErr := json.Unmarshal(msg.Key, &keyData)
			// if keyErr != nil {
			// 	kafkaError = logging.EnrichErrorWithStackTrace(keyErr)
			// }
			// kafKeyValue.Payload = keyData.Payload

			// kafKeyValue.Payload = valueData.Key
			// kafKeyValue.Event = valueData.Event
			// KafkaKeyValueList = append(KafkaKeyValueList, kafKeyValue)
		}
	}
	return KafkaKeyValueList, kafkaError
}

func CommitOffet() {
	errCommitOffset := kafka.CommitOffset(topic, consumerGroup)
	if errCommitOffset != nil {
		logging.DoLoggingLevelBasedLogs(logging.Error, "", logging.EnrichErrorWithStackTrace(errCommitOffset))
	} else {
		logging.DoLoggingLevelBasedLogs(logging.Debug, "commit offset successfully on kafka", nil)
	}
}

func GetCouchbaseDocument() {

}

func InsertCouchbaseDocument(bucketName string, prefix string, size int) {

	for i := 0; i < size; i++ {
		var sb strings.Builder
		sb.WriteString(prefix)
		sb.WriteString(strconv.Itoa(i))
		docKey := sb.String()

		var person core.Person
		person.Id = docKey
		person.Name = "Student"
		person.Age = 25
		data, err := json.Marshal(person)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Upsert record docKey: ", docKey)
			dataStr := string(data)
			err = couchbase.Raw_AddOrUpdateDocument(bucketName, docKey, dataStr)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
		// if i%10 == 0 {
		// 	fmt.Println("-----------------------------------------")
		// 	time.Sleep(2 * time.Second)
		// }
	}
}

func DeleteCouchbaseDocument(bucketName string, prefix string, size int) {
	for i := 0; i < size; i++ {
		var sb strings.Builder
		sb.WriteString(prefix)
		sb.WriteString(strconv.Itoa(i))
		docKey := sb.String()
		fmt.Println("Delete record docKey: ", docKey)
		err := couchbase.RemoveDocument(bucketName, docKey)
		if err != nil {
			fmt.Println(err.Error())
		}
		if i%10 == 0 {
			fmt.Println("-----------------------------------------")
			time.Sleep(2 * time.Second)
		}
	}
}

// func MakeMessageBatch(documentKeys map[string]string) (KeyReceiverList []core.ReceiverMetaData) {
// 	j := 0
// 	for k, v := range documentKeys {
// 		KeyReceiverList = append(KeyReceiverList, core.ReceiverMetaData{
// 			Payload:      k,
// 			ClientObject: receiverDomainsArray[j],
// 			Event:        v,
// 		})
// 		j++
// 		if j == len(receiverDomainsArray) {
// 			j = 0
// 		}
// 	}
// 	return KeyReceiverList
// }
