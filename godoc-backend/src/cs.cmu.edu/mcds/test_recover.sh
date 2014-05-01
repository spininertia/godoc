cd appserver/
go run appserver.go -config config1.txt -me 2 &
cd ../recover
go run consul.go -config config1.txt &
