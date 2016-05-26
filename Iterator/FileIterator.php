<?php
namespace HopelessCodeFiend\Geonames\Iterator;

use Exception;
use Iterator;

class FileIterator implements Iterator
{

    private $position = 0;
    private $highestPull = 0;
    private $fileHandle;
    private $fileToParse;
    private $data = [];
    private $rewindCalled = false;

    public function __construct($file_to_parse)
    {
        try {

            if (!file_exists($file_to_parse)) {
                throw new Exception('File does not exists');
            }

            $this->fileToParse = $file_to_parse;

            if (($this->fileHandle = @fopen($this->fileToParse, "r")) === false) {
                throw new Exception('no data in file to parse');
            }
        } catch (Exception $e) {
            echo $e->getMessage();
        }
    }

    public function __destruct()
    {
        if (isset($this->fileHandle)) {
            fclose($this->fileHandle);
        }
    }

    public function key()
    {
        return $this->position;
    }

    public function current()
    {
        if ($this->position === 0 && $this->rewindCalled !== true) {
            $this->pullData();
        }

        if ($this->rewindCalled === true) {
            $this->rewindCalled = false;
        }

        if (!array_key_exists($this->position, $this->data)) {
            return false;
        }

        return $this->data[$this->position];
    }

    private function pullData()
    {
        /**
         * If data already exist for position, then skip pulling from file
         * and return existing data
         */
        if ($this->position <= $this->highestPull && count($this->data) > 0) {
            return $this->data[$this->position];
        }

        /**
         * If we are pulling data that hasn't already been set then
         * increase the highest pull number and move forward with pulling new data
         */
        if ($this->position > $this->highestPull) {
            $this->highestPull = $this->position;
        }

        /**
         * Make sure we have reached the end of the file
         */
        if (feof($this->fileHandle)) {
            return false;
        }

        /**
         * Make sure we are returning data being requested
         */
        if (($data = fgets($this->fileHandle)) === false) {
            return false;
        }

        $data = explode("\t", $data);

        /**
         * Assign and return the new row after sanatizing it
         */
        return $this->data[$this->position] = array_map("utf8_encode", $data);
    }

    public function valid()
    {
        return isset($this->data[$this->position]);
    }

    public function last()
    {
        --$this->position;
    }


    public function delete()
    {
        unset($this->data[$this->position]);
        $this->next();
    }

    public function next()
    {
        ++$this->position;
        $this->pullData();
    }

    public function rewind()
    {
        $this->rewindCalled = true;

        if ($this->position === 0) {
            $this->pullData();

            return true;
        }

        $this->position = 0;
    }
}

