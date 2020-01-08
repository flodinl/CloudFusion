all:
	cp CFconfig.json $(GOPATH)/bin
	cp -r CloudFusionTests $(GOPATH)/bin
	go install .