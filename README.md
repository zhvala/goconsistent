goconsistent
==========

Consistent hash package for Go, fork from [consistent](stathat.com/c/consistent).

Installation
------------

    go get github.com/zhvala/goconsistent

Document
--------

Look at the [godoc](http://godoc.org/github.com/zhvala/goconsistent).

Example
------

```go
c := consistent.New()
c.Add("keyA", "valueA")
c.Add("keyB", "valueB")
c.Add("keyC", "valueC")
elem, err := c.Get("raw")
fmt.Println(elem.Key, elem.Value, elem.Replica)
```

About
-----

- Email zhvala@foxmail.com