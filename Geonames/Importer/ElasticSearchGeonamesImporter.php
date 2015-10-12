<?php

namespace HopelessCodeFiend\Geonames\Importer;

use Elasticsearch\ClientBuilder;
use Exception;
use HopelessCodeFiend\Geonames\DataSource;
use InvalidArgumentException;
use Iterator;

class ElasticSearchGeonamesImporter extends GeonamesImporter {

    private $client;

    public function __construct($dataSourceBase)
    {
        parent::__construct($dataSourceBase);

        $this->client = ClientBuilder::create()->build();
    }

    public function import_to_database(Iterator $iterator)
    {
        try
        {
            self::job_start();

            while ($iterator->current() !== false)
            {

                // You have the data now, so delete it to clear memory
                $params = [
                    'index' => 'geonames',
                    'type' => $this->dataSource->table,
//                    'id' => 'id',
                    'body' => $this->map_params()
                ];

                $results = $this->add_to_database($params);

                if ($results['created'] === true)
                {
                    $this->insert_count++;
                }

                $iterator->delete();

                if ($this->insert_count > (int)$this->insert_at_time)
                {
                    echo ($this->actual_insert_count += $this->insert_count) . "\n";
                    $this->insert_count = 0;
                }
            }

            self::job_done();
        }
        catch (Exception $e)
        {
            throw $e;
        }
    }

    protected function add_to_database($params)
    {

        if (!array_key_exists('body', $params))
        {
            throw new InvalidArgumentException('ElasticSearch requires params to be an array with [index,type,id,body]');
        }

        $response = $this->client->index($params);

        return $response;
    }


    protected function map_params()
    {
        $columns = $this->dataSource->get_mapped_columns();
        $report_params = [];
        $row = $this->data_iterator->current();

        // Check for invalid rows
        if (count($row) !== count($columns))
        {
            error_log('Invalid row: ' . $this->actual_insert_count . ' :: ' . $row . "\n");
            echo 'line ' . $this->actual_insert_count . ' is invalid and was skipped' . "\n";
            return null;
        }

        foreach ($columns AS $column_key => $column_val)
        {
            $report_params[$column_val] = $row[$column_key];
        }

        return $report_params;
    }
}