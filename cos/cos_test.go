package cos

import (
	"context"
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	os.Setenv("COS_SECRETID", "xxxxx")
	os.Setenv("COS_SECRETKEY", "xxxxx")
	bucket := "https://test-1257883576.cos.ap-shanghai.myqcloud.com"
	key := "test.txt"

	pw, err := NewCosFileWriter(context.Background(), bucket, key)
	if err != nil {
		t.Error(err)
	}

	n, err := pw.Write([]byte("hello world"))
	if err != nil {
		t.Error(err)
	}

	t.Log(n)

	err = pw.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestRead(t *testing.T) {
	os.Setenv("COS_SECRETID", "xxxxx")
	os.Setenv("COS_SECRETKEY", "xxxxx")
	bucket := "https://test-1257883576.cos.ap-shanghai.myqcloud.com"
	key := "test.txt"

	pr, err := NewCosFileReader(context.Background(), bucket, key)
	if err != nil {
		t.Error(err)
	}

	pr.Seek(5, 0)

	buf := make([]byte, 50)
	n, err := pr.Read(buf)
	if err != nil {
		t.Error(err)
	}

	t.Log(n)
	t.Log(string(buf))

	err = pr.Close()
	if err != nil {
		t.Error(err)
	}
}
