<?php

namespace HopelessCodeFiend\Geonames\Importer;

use HopelessCodeFiend\Geonames\DataSource;
use Iterator;

class MysqlGeonamesImporter extends GeonamesImporter
{

    private $_forceFun = false;

    public function importToDatabase(Iterator $iterator)
    {
        try {
            $j = 0;

            self::jobStart();

            while (($row = $iterator->current()) !== false) {
                $this->reportSql[]
                    = '
                    INSERT INTO
                      tb_data.'.$this->dataSource->table.'
                    SET '.$this->mapColumns($iterator->key()).'
                    ON DUPLICATE KEY UPDATE
                       '.$this->mapColumns($iterator->key(), 1);

                $this->reportParams = array_merge($this->reportParams, $this->mapParams($iterator->key()));

                $j++;
                $iterator->delete();

                $this->addToDatabase($j);
            }

            if (count($this->reportSql) > 0) {
                $this->_forceFun = true;
                $this->addToDatabase($j);
            }

            self::jobDone();

        } catch (\Exception $e) {
            throw $e;
        }
    }

    protected function addToDatabase($j)
    {
        $this->insertCount += count($this->reportSql);

        if ($this->dataSource->config->recover === true && !self::caughtUp()) {
            $this->reportSql = [];
            $this->reportParams = [];

            return null;
        }

        if ($j >= $this->insertAtTime || $this->_forceFun === true) {

            $res = call_user_func(
                [
                    $this->DB,
                    'exec',
                ],
                implode(';', $this->reportSql),
                $this->reportParams
            );

            if (!isset($res) || $res['error']) {
                die(var_export($res));
            }

            $this->actualInsertCount += count($this->reportSql);
            $this->updateCurrentProgress();
            echo $this->actualInsertCount.' inserted'."\r\n";

            $this->reportSql = [];
            $this->reportParams = [];
            $j = 0;
            $this->_forceFun = false;
        }
    }

    protected function mapColumns($count, $duplicate = null)
    {
        $columns = $this->dataSource->getMappedColumns();
        $unique_keys = $this->dataSource->getUniqueKeys();

        if (isset($duplicate)) {
            foreach ($unique_keys AS $primary_key) {
                unset($columns[$primary_key]);
            }
        }

        $results = '';

        foreach ($columns AS $column) {
            $results .= $column.' = {{'.$column.'_'.$count.'}},';
        }

        return rtrim($results, ',');
    }

    protected function mapParams($key)
    {
        $columns = $this->dataSource->getMappedColumns();
        $reportParams = [];
        $row = $this->dataIterator->current();

        // Check for invalid rows
        if (count($row) !== count($columns)) {
            error_log('Invalid row: '.$this->actualInsertCount.' :: '.$row."\n");
            echo 'line '.$this->actualInsertCount.' is invalid and was skipped'."\n";

            return null;
        }

        foreach ($columns AS $columnKey => $columnVal) {
            $reportParams[$columnVal.'_'.$key] = $row[$columnKey];
        }

        return $reportParams;
    }
}