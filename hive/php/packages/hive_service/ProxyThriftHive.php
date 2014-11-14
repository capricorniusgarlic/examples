<?php

use hiveserver2\TCLIServiceClient;
use hiveserver2\TOpenSessionReq;
use hiveserver2\TExecuteStatementReq;
use hiveserver2\TFetchResultsReq;
use hiveserver2\TStatusCode;
use hiveserver2\TFetchOrientation;
use metastore\ThriftHiveMetastoreClient;

interface ProxyHiveClientIf {
    public function execute($query);
    public function fetchOne();
    public function fetchN($numRows);
    public function fetchAll();
    public function get_table($dbname, $tbl_name);
    public function get_all_tables($dbname);
}

class ProxyThriftHiveClient implements ProxyHiveClientIf {
    private $client;
    private $metaClient;
    private $openSessionResp;
    private $execResp;

    public function __construct($CNF) {
        // set up connection to hiverserver2
        $transport = new TSocket($CNF->HIVE_THRIFT_SERVER2_HOST, $CNF->HIVE_THRIFT_SERVER2_PORT);
        $transport->setSendTimeout(30 * 1000);
        $transport->setRecvTimeout(24 * 3600 * 1000);
        $protocol = new TBinaryProtocol($transport);
        $transport->open();
        
        $this->client = new TCLIServiceClient($protocol);
        $openSessionReq = new TOpenSessionReq(array(
                'client_protocol' => 0
        ));
        $this->openSessionResp = $this->client->OpenSession($openSessionReq);
        
        // set up connection to hivemetastore
        $transport0 = new TSocket($CNF->HIVE_METASTORE_HOST, $CNF->HIVE_METASTORE_PORT);
        $transport0->setSendTimeout(30 * 1000);
        $transport0->setRecvTimeout(24 * 3600 * 1000);
        $protocol0 = new TBinaryProtocol($transport0);
        $transport0->open();
        $this->metaClient = new ThriftHiveMetastoreClient($protocol0);
    }

    public function execute($query) {
        $sessionHandle = $this->openSessionResp->sessionHandle;
        $execReq = new TExecuteStatementReq(array(
                'sessionHandle'=> $sessionHandle,
                'statement' => $query
        ));
        $execResp = $this->client->ExecuteStatement($execReq);
        $statusCode = $execResp->status->statusCode;
        if ($statusCode == TStatusCode::SUCCESS_STATUS || $statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS) {
            $this->execResp = $execResp;
        } else {
            throw new Exception(sprintf('execute failed witherror code: %d, message: %s' . "\n", $execResp->status->errorCode, $execResp->status->errorMessage));
        }
    }

    public function fetchOne() {
        if (isset($this->execResp)) {
            $operationHandle = $this->execResp->operationHandle;
            $fetchReq = new TFetchResultsReq(array(
                    'operationHandle'=> $operationHandle,
                    'orientation' => TFetchOrientation::FETCH_FIRST,
                    'maxRows' => 1
            ));
            $resultRows = $this->fetchNext($fetchReq);
            if (empty($resultRows)) {
                throw new Exception("fetchOne failed: resultRows is empty");
            }
            $resultRow = $resultRows[0];
            return $resultRow->colVals[0];
        } else {
            throw new Exception("fetchOne failed: execResp is not set " + $this->execResp);
        }
    }

    public function fetchN($numRows) {
        if (isset($this->execResp)) {
            $rows = array();
            $operationHandle = $this->execResp->operationHandle;
            $fetchReq = new TFetchResultsReq(array(
                    'operationHandle'=> $operationHandle,
                    'orientation' => TFetchOrientation::FETCH_NEXT,
                    'maxRows' => $numRows
            ));

            $resultRows = $this->fetchNext($fetchReq);
            foreach($resultRows as $resultRow) {
                $rows[] = $resultRow;
            }

            return $rows;
        } else {
            throw new Exception("fetchAll failed: execResp is not set " + $this->execResp);
        }
    }

    public function fetchAll() {
        if (isset($this->execResp)) {
            $rows = array();
            $operationHandle = $this->execResp->operationHandle;
            $fetchReq = new TFetchResultsReq(array(
                    'operationHandle'=> $operationHandle,
                    'orientation' => TFetchOrientation::FETCH_NEXT,
                    'maxRows' => 1000
            ));

            $resultRows = $this->fetchNext($fetchReq);
            while(!empty($resultRows)) {
                foreach($resultRows as $resultRow) {
                    $rows[] = $resultRow;
                }
                $resultRows = $this->fetchNext($fetchReq);
            }

            return $rows;
        } else {
            throw new Exception("fetchAll failed: execResp is not set " + $this->execResp);
        }
    }

    private function fetchNext($fetchReq) {
        $resultsResp = $this->client->FetchResults($fetchReq);
        return $resultsResp->results->rows;
    }
    
    public function get_table($dbname, $tbl_name) {
        return $this->metaClient->get_table($dbname, $tbl_name);
    }
    
    public function get_all_tables($dbname) {
        return $this->metaClient->get_all_tables($dbname);
    }
}
