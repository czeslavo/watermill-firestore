package firestore


type firestoreMessage struct {
	UUID     string                 `firestore:"uuid"`
	Metadata map[string]interface{} `firestore:"metadata"`
	Payload  []byte                 `firestore:"payload"`
}