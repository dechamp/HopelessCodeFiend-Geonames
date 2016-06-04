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

            $params = ['body' => []];
            $rowsProcessedCount = 0;

            while ($iterator->current() !== false) {
                $params['body'][] = [
                    'index' => [
                        '_index' => 'geonames',
                        '_type' => $this->dataSource->table,
                    ],
                ];

                $params['body'][] = $this->map_params();
                $rowsProcessedCount++;

                if ($rowsProcessedCount % $this->insertAtTime == 0) {

                    $responses = $this->addToDatabaseBulk($params);

                    if (array_key_exists('errors', $responses) && false !== $responses['errors']) {
                       throw new Exception('Error while inserting data');
                    }

                    fwrite(STDOUT, "\r".$rowsProcessedCount.' inserted');

                    $params = ['body' => []];
                    unset($responses);
                }
            }

            // Send the last batch if it exists
            if (!empty($params['body'])) {
                $responses = $this->addToDatabaseBulk($params);

                if (array_key_exists('errors', $responses) && false !== $responses['errors']) {
                    throw new Exception('Error while inserting data');
                }

                fwrite(STDOUT, "\r".$rowsProcessedCount.' inserted');
            }

            fwrite(STDERR, "\n".'Done! :)');
            self::jobDone();
        } catch (Exception $e) {
            throw $e;
        }
    }

    protected function addToDatabase($params = null)
    {
        if (!isset($params) || !array_key_exists('body', $params)) {
            throw new InvalidArgumentException('ElasticSearch requires params to be an array with [index,type,body]');
        }

        $response = $this->client->index($params);

        return $response;
    }

    protected function addToDatabaseBulk($params = null)
    {
        if (!isset($params) || !array_key_exists('body', $params)) {
            throw new InvalidArgumentException('ElasticSearch requires params to be an array with [index,type,body]');
        }

        $response = $this->client->bulk($params);

        return $response;
    }

    protected function map_params()
    {
        $columns = $this->dataSource->getMappedColumns();
        $report_params = [];
        $row = $this->dataIterator->current();

        // Check for invalid rows
        if (count($row) !== count($columns)) {
            error_log('Invalid row: '.$this->rowsProcessedCount.' :: '.$row."\n");
            echo 'line '.$this->rowsProcessedCount.' is invalid and was skipped'."\n";

            return null;
        }

        foreach ($columns AS $column_key => $column_val) {
            $report_params[$column_val] = $row[$column_key];
        }

        return $report_params;
    }
}