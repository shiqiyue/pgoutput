package main

import (
	"context"
	"fmt"
	"github.com/shiqiyue/pgoutput"
	"log"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

func main() {
	//	const outputPlugin = "test_decoding"
	const outputPlugin = "pgoutput"
	conn, err := pgconn.Connect(context.Background(), "postgres://postgres:root@192.168.3.132/xd_design?replication=database")
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	var pluginArguments = []string{"proto_version '1'", "publication_names 'xd_design_dm'"}
	slotName := "xd_design_dm"

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	lsn, err := pglogrepl.ParseLSN("0/1C1B8000")
	if err != nil {
		log.Fatalln("parse lsn fail", err)
	}
	_, _ = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	//if err != nil {
	//	log.Fatalln("CreateReplicationSlot failed:", err)
	//}
	log.Println("Created temporary replication slot:", slotName)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, lsn, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := lsn
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	set := pgoutput.NewRelationSet(nil)

	dump := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		r, ok := set.Get(relation)
		if ok {
			log.Println(r)
		}
		if err != nil {
			return fmt.Errorf("error parsing values: %s", err)
		}
		log.Println(relation)
		for name, value := range values {
			val := value.Get()
			log.Printf("%s (%T): %#v", name, val, val)
		}
		return nil
	}
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				m, err := pgoutput.Parse(msg.Data[25:])
				if err != nil {
					log.Println(err.Error())
				}
				switch v := m.(type) {
				case pgoutput.Relation:
					log.Printf("RELATION")
					set.Add(v)
				case pgoutput.Insert:
					log.Printf("INSERT")
					dump(v.RelationID, v.Row)
				case pgoutput.Update:
					log.Printf("UPDATE")
					dump(v.RelationID, v.Row)
				case pgoutput.Delete:
					log.Printf("DELETE")
					dump(v.RelationID, v.Row)
				}
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])

				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}

	}
	//for {
	//	if time.Now().After(nextStandbyMessageDeadline) {
	//		err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
	//		if err != nil {
	//			log.Fatalln("SendStandbyStatusUpdate failed:", err)
	//		}
	//		log.Println("Sent Standby status message")
	//		nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
	//	}
	//
	//	ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
	//	msg, err := conn.ReceiveMessage(ctx)
	//	cancel()
	//	if err != nil {
	//		if pgconn.Timeout(err) {
	//			continue
	//		}
	//		log.Fatalln("ReceiveMessage failed:", err)
	//	}
	//
	//	switch msg := msg.(type) {
	//	case *pgproto3.CopyData:
	//		switch msg.Data[0] {
	//		case pglogrepl.PrimaryKeepaliveMessageByteID:
	//			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
	//			if err != nil {
	//				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
	//			}
	//			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
	//
	//			if pkm.ReplyRequested {
	//				nextStandbyMessageDeadline = time.Time{}
	//			}
	//
	//		case pglogrepl.XLogDataByteID:
	//			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	//			if err != nil {
	//				log.Fatalln("ParseXLogData failed:", err)
	//			}
	//			log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
	//			logicalMsg, err := pglogrepl.Parse(xld.WALData)
	//			if err != nil {
	//				log.Fatalf("Parse logical replication message: %s", err)
	//			}
	//			log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
	//
	//			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	//		}
	//	default:
	//		log.Printf("Received unexpected message: %#v\n", msg)
	//	}
	//
	//}
}
