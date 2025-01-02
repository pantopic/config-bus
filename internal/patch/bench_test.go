package patch

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

var diffs = [][][]byte{
	{[]byte(`012345678`), []byte(`0123456789`)},
	{[]byte(`107e7929-774b-46f9-b785-0d95277cd138`), []byte(`f29fdb14-dbf9-455e-b05e-7bc77e702832`)},
	{[]byte(`107e7929-774b-46f9-b785-0d95277cd138.f29fdb14-dbf9-455e-b05e-7bc77e702832`), []byte(`f29fdb14-dbf9-455e-b05e-7bc77e702832.107e7929-774b-46f9-b785-0d95277cd138`)},
	{[]byte(`107e7929-774b-46f9-b785-0d95277cd138.d3083d27-fceb-496e-bdee-d9ddcf859cd0`), []byte(`f29fdb14-dbf9-455e-b05e-7bc77e702832.ae86f2b4-5263-4671-a63d-64f6520d29ce`)},
	{manifestExample1, manifestExample2},
	{manifestExample1, manifestExample3},
}

func BenchmarkGenerate(b *testing.B) {
	var i = 0
	for _, tc := range diffs {
		b.Run(fmt.Sprintf(`diff %d`, i), func(b *testing.B) {
			benchmarkGenerate(b, tc)
		})
		i++
	}
}

func benchmarkGenerate(b *testing.B, diff [][]byte) {
	b.ReportAllocs()
	var res []byte
	for i := 0; i < b.N; i++ {
		res = Generate(diff[0], diff[1], nil)
	}
	b.SetBytes(int64(len(res)))
}

func BenchmarkApply(b *testing.B) {
	var i = 0
	var err error
	var newFile []byte
	for _, diff := range diffs {
		patch := Generate(diff[0], diff[1], nil)
		b.Run(fmt.Sprintf(`patch %d`, i), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				newFile, err = Apply(diff[0], patch, newFile)
				if err != nil {
					panic(err)
				}
			}
			if !bytes.Equal(newFile, diff[1]) {
				b.Fatal("expected:\n", string(diff[1]), "got:\n", string(newFile))
			}
			b.SetBytes(int64(len(newFile)))
		})
		i++
	}
}

func TestGenerateSize(t *testing.T) {
	log.Printf("Patch size: a\t b\t patch")
	log.Printf("Patch size: --------------------------")
	for _, tc := range diffs {
		patch := Generate(tc[0], tc[1], nil)
		newFile2, err := Apply(tc[0], patch, nil)
		if err != nil {
			panic(err)
		}
		log.Printf("Patch size: %d\t %d\t %d", len(tc[0]), len(tc[1]), len(patch))
		if !bytes.Equal(tc[1], newFile2) {
			t.Fatalf("Unmatched: %s - %s", string(tc[0]), string(newFile2))
		}
	}
}

var manifestExample1 = []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node-type
                operator: In
                values:
                - nginx-node
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
                - nginx
            topologyKey: "kubernetes.io/hostname"
      containers:
      - name: nginx-container
        image: nginx:latest
        ports:
        - containerPort: 80
`)

var manifestExample2 = []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
spec:
  replicas: 6
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node-type
                operator: In
                values:
                - nginx-node
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
                - nginx
            topologyKey: "kubernetes.io/hostname"
      containers:
      - name: nginx-container
        image: nginx:latest
        ports:
        - containerPort: 80
`)

var manifestExample3 = []byte(`apiVersion: apps/v1
kind: Deployment1
metadata:
  name: nginx-deployment1
spec:
  replicas: 61
  selector:
    matchLabels:
      app: nginx1
  template:
    metadata:
      labels:
        app: nginx
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node-type1
                operator: In
                values:
                - nginx-node1
            - matchExpressions:
              - key: node-type1
                operator: In
                values:
                - nginx-node1
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In1
                values:
                - nginx1
            topologyKey: "kubernetes.io/hostname"
      containers:
      - name: nginx-container
        image: nginx:latest1
        ports:
        - containerPort: 80
`)
