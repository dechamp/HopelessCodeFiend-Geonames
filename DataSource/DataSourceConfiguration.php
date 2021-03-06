<?php

namespace HopelessCodeFiend\Geonames\DataSource;


use InvalidArgumentException;

class DataSourceConfiguration
{
    public $recover = false;
    private $fileUrl;
    private $dataFileWithin;
    private $tempDirectory;
    private $database;
    private $databaseName = 'geonames';
    private $maxResourceLifeSpan = (60 * 60 * 24);

    public function __construct($pathToFile, $dataFileWithin)
    {
        if (empty($pathToFile) || empty($dataFileWithin)) {
            throw new InvalidArgumentException;
        }

        $this->fileUrl = $pathToFile;
        $this->dataFileWithin = $dataFileWithin;
    }

    public function enableRecovery()
    {
        return $this->recover = true;
    }

    public function getFileUrl()
    {
        return $this->fileUrl;
    }

    public function getDataFileName()
    {
        return $this->dataFileWithin;
    }

    public function getTempDirectory()
    {
        if ($this->tempDirectory && file_exists($this->tempDirectory)) {
            return $this->tempDirectory;
        }

        if (empty($this->tempDirectory)) {
            $this->tempDirectory = '/tmp';
        }

        if (!file_exists($this->tempDirectory)) {
            mkdir($this->tempDirectory, 0777, true);
        }

        return $this->tempDirectory;
    }

    public function setTempDirectory($tempDirectory)
    {
        $this->tempDirectory = $tempDirectory;
    }

    public function getDatabase()
    {
        return $this->database;
    }

    public function setDatabase($database)
    {
        $this->database = $database;
    }

    public function getDatabaseName()
    {
        return $this->databaseName;
    }

    public function setDatabaseName($name)
    {
        $this->databaseName = $name;
    }

    public function getMaxResourceLifeSpan()
    {
        return $this->maxResourceLifeSpan;
    }

    public function setMaxResourceLifeSpan($time)
    {
        $this->maxResourceLifeSpan = $time;
    }
}