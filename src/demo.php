<?php
require 'vendor/autoload.php';
use Phpes\Config;
use Phpes\Esdb;

class MyModel extends Esdb
{
    public $index = 'tjy';
    public $type = 'orders';
}

$model = new MyModel();
$model->select()->limit(5)->all();
var_dump($model->result);
