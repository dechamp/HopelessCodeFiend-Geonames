<?php

namespace HopelessCodeFiend\Geonames\Importer;

use HopelessCodeFiend\Geonames\DataSource;
use HopelessCodeFiend\Geonames\DataSource\DataSourceBase;
use Iterator;


abstract class GeonamesImporter
{
    protected $insertAtTime = 500;
    protected $rowsToProcess = 0;
    protected $reportSql = [];
    protected $reportParams = [];
    protected $dataSource;
    protected $dataIterator;
    protected $DB;
    protected $databaseConfig;
    public $currentProgressFilePath = '/current_progress';
    public $insertCount = 0;
    public $actualInsertCount = 0;

    public function __construct(DataSourceBase $dataSource, $databaseConfig = null)
    {
        $this->dataSource = $dataSource;
        $this->dataIterator = $this->dataSource->getDataIterator();
        $this->DB = $this->dataSource->DB();
        $this->databaseConfig = $databaseConfig;
    }

    abstract public function importToDatabase(Iterator $iterator);

    public function import()
    {
        $this->importToDatabase($this->dataIterator);
    }

    public function importCount()
    {
        return $this->insertCount;
    }

    public function insertAtTime($insert_at_time = 500)
    {
        $this->insertAtTime = $insert_at_time;
    }

    public function __toString()
    {
        $output = '';

        foreach ($this->dataIterator AS $row_key => $row_val) {
            $output .= $row_key.': '.$this->rowToString($row_val)."\n\r<hr>";
        }

        return $output;
    }

    public function jobStart()
    {
        if (!file_exists($this->dataSource->config->getTempDirectory().$this->currentProgressFilePath)) {
            file_put_contents($this->dataSource->config->getTempDirectory().$this->currentProgressFilePath, 0);
        }
    }

    public function jobDone()
    {
        unlink($this->dataSource->config->getTempDirectory().$this->currentProgressFilePath);
    }

    public function updateCurrentProgress()
    {
        file_put_contents($this->dataSource->config->getTempDirectory().$this->currentProgressFilePath, $this->insertCount);
    }

    public function caughtUp()
    {
        $count = trim(file_get_contents($this->dataSource->config->getTempDirectory().$this->currentProgressFilePath));

        return ($this->insertCount) >= (integer)$count ? true : false;
    }

    abstract protected function addToDatabase($data);

    private function rowToString($row)
    {
        $output = '';

        if (!is_array($row)) {
            return;
        }

        foreach ($row AS $key => $val) {
            $output .= $key.': '.$val."\n";
        }

        return $output;
    }
}