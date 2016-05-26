<?php

namespace HopelessCodeFiend\Geonames\Importer;

use Elasticsearch\ClientBuilder;
use Exception;
use HopelessCodeFiend\Geonames\DataSource;
use InvalidArgumentException;
use Iterator;

class ElasticSearchGeonamesImporter extends GeonamesImporter
{

    private $client;

    public function __construct($dataSourceBase, $databaseConfig = null)
    {
        parent::__construct($dataSourceBase, $databaseConfig);

        $this->client = ClientBuilder::create()->setHosts(['192.168.17.17:9200'])->build();
    }

    public function importToDatabase(Iterator $iterator)
    {
        try {
            self::jobStart();

            while ($iterator->current() !== false) {

                // You have the data now, so delete it to clear memory
                $params = [
                    'index' => 'geonames',
                    'type' => $this->dataSource->table,
//                    'id' => 'id',
                    'body' => $this->map_params(),
                ];

                $results = $this->addToDatabase($params);

                if ($results['created'] === true) {
                    $this->insertCount++;
                }

                $iterator->delete();

                if ($this->insertCount > (int)$this->insertAtTime) {
                    echo ($this->actualInsertCount += $this->insertCount)."\n";
                    $this->insertCount = 0;
                }
            }

            self::jobDone();
        } catch (Exception $e) {
            throw $e;
        }
    }

    protected function addToDatabase($params)
    {

        if (!array_key_exists('body', $params)) {
            throw new InvalidArgumentException('ElasticSearch requires params to be an array with [index,type,id,body]');
        }

        $response = $this->client->index($params);

        return $response;
    }

    protected function map_params()
    {
        $columns = $this->dataSource->getMappedColumns();
        $report_params = [];
        $row = $this->dataIterator->current();

        // Check for invalid rows
        if (count($row) !== count($columns)) {
            error_log('Invalid row: '.$this->actualInsertCount.' :: '.$row."\n");
            echo 'line '.$this->actualInsertCount.' is invalid and was skipped'."\n";

            return null;
        }

        foreach ($columns AS $column_key => $column_val) {
            $report_params[$column_val] = $row[$column_key];
        }

        return $report_params;
    }
}