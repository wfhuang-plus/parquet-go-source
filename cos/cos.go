package cos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/xitongsys/parquet-go/source"
)

//var lts time.Time = time.Now()

type CosFile struct {
	ctx    context.Context
	client *cos.Client

	// write-related fields
	writeOpened bool
	writeDone   chan error
	pipeWriter  *io.PipeWriter

	// read-related fields
	readOpened bool
	fileSize   int64
	offset     int64
	whence     int

	err error
	key string
}

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	errFailedUpload  = errors.New("Write: failed upload")
)

func NewCosFileWriterWithClient(ctx context.Context, cosClient *cos.Client, key string) (source.ParquetFile, error) {
	file := &CosFile{
		ctx:    ctx,
		client: cosClient,
	}

	return file.Create(key)
}

func NewCosFileWriter(ctx context.Context, baseUrl string, key string) (source.ParquetFile, error) {
	cosClient, err := newClient(baseUrl)
	if err != nil {
		return nil, err
	}
	return NewCosFileWriterWithClient(ctx, cosClient, key)
}

func NewCosFileReaderWithClient(ctx context.Context, cosClient *cos.Client, key string) (source.ParquetFile, error) {
	file := &CosFile{
		ctx:    ctx,
		client: cosClient,
	}
	return file.Open(key)
}

func NewCosFileReader(ctx context.Context, baseUrl string, key string) (source.ParquetFile, error) {
	cosClient, err := newClient(baseUrl)
	if err != nil {
		return nil, err
	}
	return NewCosFileReaderWithClient(ctx, cosClient, key)
}

func newClient(strUrl string) (*cos.Client, error) {
	bucketUrl, err := url.Parse(strUrl)
	if err != nil {
		return nil, err
	}
	baseUrl := &cos.BaseURL{BucketURL: bucketUrl}

	cosClient := cos.NewClient(baseUrl, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  os.Getenv("COS_SECRETID"),
			SecretKey: os.Getenv("COS_SECRETKEY"),
		},
	})
	return cosClient, nil
}

func (this *CosFile) Seek(offset int64, whence int) (int64, error) {
	if !this.readOpened {
		return 0, errors.New("file not readOpened")
	}

	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	switch whence {
	case io.SeekCurrent:
		offset += this.offset
	case io.SeekEnd:
		offset += this.fileSize
	}

	if offset < 0 || offset > this.fileSize {
		return 0, errInvalidOffset
	}
	this.offset = offset

	return this.offset, nil
}

func (this *CosFile) Read(p []byte) (int, error) {
	//sts := time.Now()

	if !this.readOpened {
		return 0, errors.New("file not readOpened")
	}

	if this.offset >= this.fileSize {
		return 0, io.EOF
	}

	ln := len(p)
	if ln < 1 {
		return 0, nil
	}

	start := this.offset
	end := start + int64(ln) - 1
	if end > this.fileSize-1 {
		end = this.fileSize - 1
	}

	opt := &cos.ObjectGetOptions{
		Range: fmt.Sprintf("bytes=%d-%d", start, end),
	}

	resp, err := this.client.Object.Get(this.ctx, this.key, opt)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	cnt := 0
	for cnt < ln {
		n, err := resp.Body.Read(p[cnt:])
		//fmt.Println(n, err)
		cnt += n
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, err
		}
	}

	this.offset += int64(cnt)

	return cnt, nil
}

func (this *CosFile) Open(key string) (source.ParquetFile, error) {
	if key == "" {
		key = this.key
	}

	var fileSize int64
	if key == this.key && this.readOpened {
		fileSize = this.fileSize
	} else {
		resp, err := this.client.Object.Head(this.ctx, key, nil)
		if err != nil {
			return nil, err
		}
		contentLength := resp.Header.Get("Content-Length")
		fileSize, err = strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return nil, err
		}

		if fileSize < 0 {
			return nil, errors.New("error file size")
		}
	}

	cosFile := &CosFile{
		ctx:        this.ctx,
		client:     this.client,
		key:        key,
		readOpened: true,
		fileSize:   fileSize,
		offset:     0,
	}

	return cosFile, nil
}

func (this *CosFile) Write(p []byte) (int, error) {
	if !this.writeOpened {
		return 0, errors.New("file not writeOpened")
	}

	if this.err != nil {
		return 0, this.err
	}

	n, err := this.pipeWriter.Write(p)
	if err != nil {
		this.err = err
		return 0, err
	}

	return n, nil
}

func (this *CosFile) Close() error {
	if this.pipeWriter != nil {
		if err := this.pipeWriter.Close(); err != nil {
			return err
		}
	}

	if this.writeDone != nil {
		<-this.writeDone
		close(this.writeDone)
		this.writeDone = nil
	}

	return this.err
}

func (this *CosFile) Create(key string) (source.ParquetFile, error) {
	pipeReader, pipeWriter := io.Pipe()
	done := make(chan error)

	cosFile := &CosFile{
		ctx:         this.ctx,
		client:      this.client,
		key:         key,
		pipeWriter:  pipeWriter,
		writeOpened: true,
		writeDone:   done,
	}

	go func() {
		_, err := cosFile.client.Object.Put(cosFile.ctx, key, pipeReader, nil)
		if err != nil {
			fmt.Println(err)
			cosFile.err = err
			pipeReader.CloseWithError(err)
		}
		done <- err
	}()

	return cosFile, nil
}
