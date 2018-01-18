<?php
use Phpes\Config;
use Phpes\Esdb;

class MyModel extends Esdb
{
    public $index = 'mydb';
    public $type = 'mytable';
}

$model = new MyModel();
$model->select()->limit(5)->all();
var_dump($es->result);
