package influxdbClientPool

import (
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

type InfluxdbPool struct {
	pool chan client.Client
	MaxSize,ActivitySize,InSize int
	httpConfig client.HTTPConfig
	udpConfig client.UDPConfig
	ConfigType string
}

func NewHttpPool(config client.HTTPConfig,maxSize int) (*InfluxdbPool,error) {
	pool:=&InfluxdbPool{pool:make(chan client.Client, maxSize),httpConfig:config,MaxSize:maxSize,ConfigType:"http"}
	clients := make([]client.Client, 0, maxSize)
	var c client.Client
	var err error
	for i := 0; i < pool.MaxSize; i++ {
		c,err=client.NewHTTPClient(pool.httpConfig)
		if err != nil {
			for _, c = range clients {
				c.Close()
			}
			clients=clients[:0]
			break
		}
		clients=append(clients,c)
	}
	pool.ActivitySize=0
	pool.InSize=len(clients)
	for _,c=range clients{
		pool.pool <- c
	}
	return pool,err
}

func NewUdpPool(config client.UDPConfig,maxSize int) (*InfluxdbPool,error) {
	pool:=&InfluxdbPool{pool:make(chan client.Client, maxSize),udpConfig:config,MaxSize:maxSize,ConfigType:"udp"}
	clients := make([]client.Client, 0, maxSize)
	var c client.Client
	var err error
	for i := 0; i < pool.MaxSize; i++ {
		c,err=client.NewUDPClient(pool.udpConfig)
		if err != nil {
			for _, c = range clients {
				c.Close()
			}
			clients=clients[:0]
			break
		}
		clients=append(clients,c)
	}
	pool.ActivitySize=0
	pool.InSize=len(clients)
	for _,c=range clients{
		pool.pool <- c
	}
	return pool,err
}

func (p *InfluxdbPool) Get()  (client.Client,error){
	var c client.Client
	select {
	case c = <-p.pool:
		p.InSize-=1
		_,_,err:=c.Ping(time.Millisecond*100)
		if err!=nil{
			c.Close()
			return p.Get()
		}
		p.ActivitySize+=1
		return c, nil
	default:
		if p.ActivitySize+p.InSize<p.MaxSize{
			p.ActivitySize+=1

			var err error
			if p.ConfigType=="http" {
				c, err = client.NewHTTPClient(p.httpConfig)
			}else {
				c, err = client.NewUDPClient(p.udpConfig)
			}
			if err!=nil{
				p.ActivitySize-=1
				return c,err
			}
			_,_,err=c.Ping(time.Millisecond*100)
			if err!=nil{
				p.ActivitySize-=1
				c.Close()
			}
			return c,err
		}else {
			time.Sleep(time.Millisecond*50)
			return p.Get()
		}
	}

}

func (p *InfluxdbPool) Put(c client.Client)  {
	p.ActivitySize-=1
	_,_,err:=c.Ping(time.Millisecond*100)
	if err!=nil{
		c.Close()
		return
	}
	p.InSize+=1
	p.pool<-c

}



func (p *InfluxdbPool) Write(bp client.BatchPoints)  (error){
	c,err:=p.Get()
	if err!=nil{
		return err
	}
	err=c.Write(bp)
	p.Put(c)
	return err
}

func (p *InfluxdbPool) Query(q client.Query)  (*client.Response,error){
	c,err:=p.Get()
	if err!=nil{
		return nil,err
	}
	rs,err:=c.Query(q)
	p.Put(c)
	return rs,err
}

