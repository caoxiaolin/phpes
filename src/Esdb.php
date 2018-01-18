<?php
namespace Phpes;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7;

/**
 * class Esdb
 */
class Esdb
{
    /**
     * Elasticsearch index, mysql database name
     * @var string
     */
    public $index;

    /**
     * Elasticsearch type, mysql table name
     * @var string
     */
    public $type;

    /**
     * @var array
     */
    private $_params = [];

    /**
     * Mysql results
     * @var array
     */
    public $result = [];

    /**
     * Elasticsearch results
     * @var array
     */
    private $_esResult = [];

    /**
     * generated SQL statements, requires elasticsearch-sql support
     * @see https://github.com/NLPchina/elasticsearch-sql
     * @var string
     */
    private $_sql;

    /**
     * The URL to request elasticsearch
     * @var string
     */
    private $_url;

    /**
     * @param $funcname string
     * @param $args array
     * @return this
     */
    public function __call(string $funcname, array $args):\Phpes\Esdb
    {
        $funcname = strtolower($funcname);
        switch ($funcname)
        {
            case 'select':
                $this->_params['fields'] = $args[0];
                break;
            case 'where':
                $this->_params['where'] = $args[0];
                break;
            case 'groupby':
                $this->_params['groupby'] = $args[0];
                break;
            case 'having':
                $this->_params['having'] = $args[0];
                break;
            case 'orderby':
                $this->_params['orderby'] = $args[0];
                break;
            case 'limit':
                if (count($args) === 1)
                {
                    $this->_params['offset'] = 0;
                    $this->_params['limit'] = $args[0];
                }else{
                    $this->_params['offset'] = $args[0];
                    $this->_params['limit'] = $args[1];
                }
                break;
            case 'count':
                $this->_params['fields'] = 'COUNT(*) AS cnt';
                $this->result = $this->_getCount();
                break;
            case 'all':
                $this->result = $this->_getAll();
                break;
            case 'one':
                $this->_params['limit'] = 1;
                $this->result = $this->_getOne();
                break;
        }
        return $this;
    }

    /**
     * build mysql query sql
     * @return this
     */
    private function _buildSql():\Phpes\Esdb
    {
        $this->_sql = 'SELECT ';
        $this->_sql.= $this->_params['fields'] ? $this->_params['fields'] : '*';
        $this->_sql.= ' FROM ' . $this->index . '/' . $this->type . ' ';
        $this->_sql.= $this->_params['where'] ? 'WHERE ' . $this->_params['where'] . ' ' : '';
        $this->_sql.= $this->_params['groupby'] ? 'GROUP BY ' . $this->_params['groupby'] . ' ': '';
        $this->_sql.= $this->_params['having'] ? 'HAVING ' . $this->_params['having'] . ' ' : '';
        $this->_sql.= $this->_params['orderby'] ? 'ORDER BY ' . $this->_params['orderby'] . ' ' : '';
        if ($this->_params['offset'] && $this->_params['limit']) {
            $this->_sql.= 'LIMIT ' . $this->_params['offset'] . ', ' . $this->_params['limit'];
        }
        elseif ($this->_params['limit']) {
            $this->_sql.= 'LIMIT ' . $this->_params['limit'];
        }
        return $this;
    }

    /**
     * build elasticsearch query url
     * @return this
     */
    private function _buildUrl():\Phpes\Esdb
    {
        $this->_url = Config :: $esConfig['host'] . '/_sql?sql=' . $this->_sql;
        return $this;
    }

    /**
     * get list
     * @return array
     */
    private function _getAll():array
    {
        $result = [];
        if ($this->_exec() && $this->_esResult['hits']['hits'])
        {
            foreach ($this->_esResult['hits']['hits'] as $item)
            {
                $result[] = $item['_source'];
            }
        }
        return $result;
    }

    /**
     * get one
     * @return array
     */
    private function _getOne():array
    {
        $result = [];
        if ($this->_exec() && $this->_esResult['hits']['hits'])
        {
            $result = $this->_esResult['hits']['hits'][0]['_source'];
        }
        return $result;
    }

    /**
     * get count
     * @return int
     */
    private function _getCount():int
    {
        if ($this->_exec())
        {
            return $this->_esResult['aggregations']['cnt']['value'];
        }
        return 0;
    }

    /**
     * @return bool
     */
    private function _exec():bool
    {
        $this->_buildSql();
        $this->_buildUrl();

        $client = new \GuzzleHttp\Client();

        try {
            $res = $client->request('GET', $this->_url, ['auth' => [Config :: $esConfig['username'], Config :: $esConfig['password']]]);
        } catch (RequestException $e) {
            throw new Exception(Psr7\str($e->getResponse()));
            return false;
        }

        $result = $res->getBody();
        $this->_esResult = json_decode($result, true);

        //clear $this->_params
        $this->_params = [];

        return true;
    }
}
