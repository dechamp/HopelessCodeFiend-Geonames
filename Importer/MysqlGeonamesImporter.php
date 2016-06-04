<?php

namespace HopelessCodeFiend\Geonames\Importer;

use Exception;
use HopelessCodeFiend\Geonames\DataSource;
use Iterator;
use PDOException;

class MysqlGeonamesImporter extends GeonamesImporter
{
    private $_forceRun = false;
    private $processedAt = 0;

    public function importToDatabase(Iterator $iterator)
    {
        try {
            self::jobStart();
            $this->process($iterator);
            self::jobDone();
        } catch (Exception $e) {
            throw $e;
        }
    }

    protected function process(Iterator $iterator)
    {
        fwrite(STDOUT, "\n".'Building query from file, parsing in to chunks of '.$this->insertAtTime."\n\r");

        while (true) {
            $this->generateQuery($iterator);
            $this->addToDatabase();
            if (!$iterator->valid()) {
                break;
            }
        }

        fwrite(
            STDOUT,
            "\r".$this->rowsAffectedCount.' rows affected (query uses "replace", if row existed
            it will replace it which counts as 2 (delete, insert). Don\'t let the count concern you.)'."\n"
        );
        fwrite(STDOUT, 'Done! :)'."\n\r");
    }

    protected function generateQuery(Iterator $iterator)
    {
        // TODO FIX
//        if ($this->dataSource->getConfig()->recover === true && !self::caughtUp()) {
//            $this->reportSql = '';
//            $this->reportParams = [];
//
//            return null;
//        }

        $rowCount = 0;
        $reportParams = [];
        $query = '';
        $query .= 'REPLACE INTO';
        $query .= ' '.$this->dataSource->getConfig()->getDatabaseName().'.'.$this->dataSource->table;
        $query .= ' ('.$this->mapColumns().')';
        $query .= ' VALUES ';

        while ($iterator->current() && $iterator->valid() !== false && $rowCount < $this->insertAtTime) {
            $query .= '('.$this->mapColumnPlaceHolders().'),';
            $reportParams = array_merge($reportParams, $this->mapParams());
            $rowCount++;
            $this->rowsProcessedCount++;
            $iterator->delete();
        }

        $this->reportSql[] = $query;
        $this->reportParams[] = $reportParams;
    }

    protected function addToDatabase()
    {
        try {

            foreach ($this->reportSql AS $reportSqlKey => $reportSql) {
                // Cleanup the query
                $reportSql = rtrim($reportSql, ',');

                // Run query and track stats
                $statement = $this->database->prepare($reportSql);
                $statement->execute($this->reportParams[$reportSqlKey]);
                $affected_rows = $statement->rowCount();
                $this->rowsAffectedCount += $affected_rows;
                $this->processedAt += $this->insertAtTime;
                fwrite(STDOUT, "\r".$this->processedAt.' rows inserted');

                if (false === $this->updateCurrentProgress()) {
                    fwrite(STDERR, 'Unable to track progress, check file permissions'."\n");
                }

                // Reset attributes
                unset($this->reportSql[$reportSqlKey]);
                unset($this->reportParams[$reportSqlKey]);
            }
        } catch (PDOException $e) {
            throw $e;
        }

        if (null === $this->database->lastInsertId()) {
            throw new Exception($this->database->errorInfo());
        }
    }

    protected function mapColumns()
    {
        $columns = $this->dataSource->getMappedColumns();
        $results = '';

        foreach ($columns AS $column) {
            $results .= $column.',';
        }

        return rtrim($results, ',');
    }

    protected function mapColumnPlaceHolders()
    {
        $columns = $this->dataSource->getMappedColumns();
        $results = '';

        foreach ($columns AS $column) {
            $results .= '?,';
        }

        return rtrim($results, ',');
    }

    protected function mapParams()
    {
        $report_params = [];
        $columns = $this->dataSource->getMappedColumns();
        $row = $this->dataIterator->current();

        // Check for invalid rows
        if (count($row) !== count($columns)) {
            error_log('Invalid row: '.$this->rowsProcessedCount.' :: '.$row."\n");
            fwrite(STDOUT, 'line '.$this->rowsProcessedCount.' is invalid and was skipped'."\n");

            return null;
        }

        foreach ($columns AS $column_key => $column_val) {
            $report_params[] = $row[$column_key];
        }

        return $report_params;
    }
}