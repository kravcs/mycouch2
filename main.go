package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/couchbase/gocb.v1"
)

type Person struct {
	ID        string `json:"id,omitempty"`
	Firstname string `json:"firstname,omitempty"`
	Lastname  string `json:"lastname,omitempty"`
	Email     string `json:"email,omitempty"`
}

type N1qlPerson struct {
	Person Person `json:"person"`
}

var bucket *gocb.Bucket

func getPeopleEndpoint(w http.ResponseWriter, req *http.Request) {
	var person []Person
	query := gocb.NewN1qlQuery("SELECT * FROM example AS person")
	rows, _ := bucket.ExecuteN1qlQuery(query, nil)
	var row N1qlPerson
	for rows.Next(&row) {
		person = append(person, row.Person)
	}
	json.NewEncoder(w).Encode(person)
}

func getPersonEndpoint(w http.ResponseWriter, req *http.Request) {
	var n1qlParams []interface{}
	var row N1qlPerson

	query := gocb.NewN1qlQuery("SELECT * FROM example AS person WHERE META(person).id = $1")

	params := mux.Vars(req)
	n1qlParams = append(n1qlParams, params["id"])

	rows, _ := bucket.ExecuteN1qlQuery(query, n1qlParams)
	rows.One(&row)

	json.NewEncoder(w).Encode(row.Person)
}

func createPersonEndpoint(w http.ResponseWriter, req *http.Request) {
	var person Person
	var n1qlParams []interface{}
	_ = json.NewDecoder(req.Body).Decode(&person)

	query := gocb.NewN1qlQuery("INSERT INTO example (KEY, VALUE) values ($1, {'firstname': $2, 'lastname': $3, 'email': $4})")
	n1qlParams = append(n1qlParams, uuid.Must(uuid.NewV4()))
	n1qlParams = append(n1qlParams, person.Firstname)
	n1qlParams = append(n1qlParams, person.Lastname)
	n1qlParams = append(n1qlParams, person.Email)

	_, err := bucket.ExecuteN1qlQuery(query, n1qlParams)

	if err != nil {
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	json.NewEncoder(w).Encode(person)
}

func updatePersonEndpoint(w http.ResponseWriter, req *http.Request) {
	var person Person
	var n1qlParams []interface{}
	_ = json.NewDecoder(req.Body).Decode(&person)

	query := gocb.NewN1qlQuery("UPDATE example USE KEYS $1 SET firstname = $2, lastname = $3, email = $4")

	params := mux.Vars(req)
	n1qlParams = append(n1qlParams, params["id"])
	n1qlParams = append(n1qlParams, person.Firstname)
	n1qlParams = append(n1qlParams, person.Lastname)
	n1qlParams = append(n1qlParams, person.Email)

	_, err := bucket.ExecuteN1qlQuery(query, n1qlParams)

	if err != nil {
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	json.NewEncoder(w).Encode(person)
}

func deletePersonEndpoint(w http.ResponseWriter, req *http.Request) {
	var n1qlParams []interface{}

	query := gocb.NewN1qlQuery("DELETE FROM example AS person WHERE META(person).id = $1")

	params := mux.Vars(req)
	n1qlParams = append(n1qlParams, params["id"])

	_, err := bucket.ExecuteN1qlQuery(query, n1qlParams)

	if err != nil {
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	json.NewEncoder(w).Encode(&Person{})
}

func main() {
	router := mux.NewRouter()

	cbConnect()

	router.HandleFunc("/people", getPeopleEndpoint).Methods("GET")
	router.HandleFunc("/person", createPersonEndpoint).Methods("PUT")
	router.HandleFunc("/person/{id}", getPersonEndpoint).Methods("GET")
	router.HandleFunc("/person/{id}", updatePersonEndpoint).Methods("POST")
	router.HandleFunc("/person/{id}", deletePersonEndpoint).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":54321", router))
}

func cbConnect() {
	cluster, err := gocb.Connect("couchbase://127.0.0.1")
	if err != nil {
		fmt.Println("ERROR CONNECTING TO CLUSTER:", err)
	}

	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "admin",
		Password: "admin123",
	})

	bucket, err = cluster.OpenBucket("example", "")
	if err != nil {
		fmt.Println("ERROR OPENING BUCKET:", err)
	}
}
