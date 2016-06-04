<?php

namespace HopelessCodeFiend\Geonames\Importer;

use Exception;
use HopelessCodeFiend\Geonames\DataSource;
use HopelessCodeFiend\Geonames\DataSource\DataSourceBase;
use Iterator;
use PDO;

abstract class GeonamesImporter
{
    protected $insertAtTime = 500;
    protected $rowsToProcess = 0;
    protected $reportSql = [];
    protected $reportParams = [];
    protected $dataSource;
    protected $dataIterator;

    /**
     * @var $database PDO
     */
    protected $database;
    protected $databaseConfig;
    public $currentProgressFilePath = '/current_progress.data';
    public $rowsProcessedCount = 0;
    public $rowsAffectedCount = 0;

    public function __construct(DataSourceBase $dataSource, $databaseConfig = null)
    {
        $this->dataSource = $dataSource;
        $this->dataIterator = $this->dataSource->getDataIterator();
        $this->database = $this->dataSource->getDatabase();
        $this->databaseConfig = $databaseConfig;
    }

    abstract public function importToDatabase(Iterator $iterator);

    public function import()
    {
        $this->importToDatabase($this->dataIterator);
    }

    public function insertAtTime($insert_at_time = 500)
    {
        $this->insertAtTime = $insert_at_time;
    }

    public function jobStart()
    {
        if (!file_exists($this->dataSource->getConfig()->getTempDirectory().$this->currentProgressFilePath)) {
            file_put_contents($this->dataSource->getConfig()->getTempDirectory().$this->currentProgressFilePath, 0);
        }
    }

    public function jobDone()
    {
        unlink($this->dataSource->getConfig()->getTempDirectory().$this->currentProgressFilePath);
    }

    public function updateCurrentProgress()
    {
        try {
            if (file_exists($this->getProgressFile())
                && is_writable($this->getProgressFile())
            ) {
                return $this->updateCurrentProgressFile();
            }

            if (!file_exists($this->dataSource->getConfig()->getTempDirectory())) {
                mkdir($this->dataSource->getConfig()->getTempDirectory());
            }

            if (!is_writable($this->getProgressFile())) {
                chmod($this->getProgressFile(), 777);
            }

            return $this->updateCurrentProgressFile();

        } catch (Exception $e) {
            throw $e;
        }
    }

    public function getRowsProcessedCount()
    {
        return $this->rowsProcessedCount;
    }

    private function updateCurrentProgressFile()
    {
        return (false !== file_put_contents($this->getProgressFile(), $this->rowsProcessedCount));
    }

    private function getProgressFile()
    {
        return $this->dataSource->getConfig()->getTempDirectory().$this->currentProgressFilePath;
    }

    public function caughtUp()
    {
        $count = trim(file_get_contents($this->dataSource->getConfig()->getTempDirectory().$this->currentProgressFilePath));

        return ($this->rowsProcessedCount) >= (integer)$count ? true : false;
    }

    abstract protected function addToDatabase();

    private function rowToString($row)
    {
        $output = '';

        if (!is_array($row)) {
            return null;
        }

        foreach ($row AS $key => $val) {
            $output .= $key.': '.$val."\n";
        }

        return $output;
    }

    public function __toString()
    {
        $output = '';

        foreach ($this->dataIterator AS $row_key => $row_val) {
            $output .= $row_key.': '.$this->rowToString($row_val)."\n\r<hr>";
        }

        return $output;
    }
}