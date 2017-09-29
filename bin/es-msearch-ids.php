#!/usr/bin/env php
<?php
/**
 * Created by PhpStorm.
 * User: iwai
 * Date: 2017/02/03
 * Time: 13:05
 */

ini_set('date.timezone', 'Asia/Tokyo');

if (PHP_SAPI !== 'cli') {
    echo sprintf('Warning: %s should be invoked via the CLI version of PHP, not the %s SAPI'.PHP_EOL, $argv[0], PHP_SAPI);
    exit(1);
}

require_once __DIR__.'/../vendor/autoload.php';

use CHH\Optparse;

$parser = new Optparse\Parser();

function usage() {
    global $parser;
    fwrite(STDERR, "{$parser->usage()}\n");
    exit(1);
}

$parser->setExamples([
    sprintf("%s --host 127.0.0.1 --index shop --type book ./id_list.txt", $argv[0]),
]);

$index = null;
$type  = null;
$vsize = null;
$qsize = null;

$host  = null;
$port  = null;

$parser->addFlag('help', [ 'alias' => '-h' ], 'usage');
$parser->addFlag('verbose', [ 'alias' => '-v' ]);

$parser->addFlagVar('host', $host, [ 'has_value' => true, 'required' => true ]);
$parser->addFlagVar('port', $port, [ 'has_value' => true, 'required' => false, 'default' => 9200 ]);

$parser->addFlagVar('index', $index, [ 'has_value' => true, 'required' => true ]);
$parser->addFlagVar('type',  $type,  [ 'has_value' => true, 'required' => true ]);
$parser->addFlagVar('vsize', $vsize, [ 'has_value' => true, 'default' => 100 ]);
$parser->addFlagVar('qsize', $qsize, [ 'has_value' => true, 'default' => 100 ]);

$parser->addArgument('file', [ 'required' => false ]);

try {
    $parser->parse();
} catch (\Exception $e) {
    usage();
}

$file_path = $parser['file'];

try {
    if (!$host || !$index || !$type) {
        usage();
    }

    if ($file_path) {
        if (($fp = fopen($file_path, 'r')) === false) {
            die('Could not open '.$file_path);
        }
    } else {
        if (($fp = fopen('php://stdin', 'r')) === false) {
            usage();
        }
        $read = [$fp];
        $w = $e = null;
        $num_changed_streams = stream_select($read, $w, $e, 1);

        if (!$num_changed_streams) {
            usage();
        }
    }

    $client = $body = null;
    $_msearch_query = '';
    $_id_values     = [];
    $count = 0;

    if ($port) {
        $elastic_host = sprintf('%s:%s', $host, $port);
    } else {
        $elastic_host = $host;
    }
    $client = new GuzzleHttp\Client([
        'base_uri' => sprintf('http://%s/', $elastic_host),
        'timeout'  => 3.0,
    ]);

    while (!feof($fp)) {
        $line = trim(fgets($fp));

        if (empty($line)) {
            continue;
        }

        $_id_values[] = $line;

        if (count($_id_values) < $vsize) {
            continue;
        }

        // {"index" : "" }
        // {"size":1,"query":{"ids":{"values":["",""]}}}
        $_msearch_query .= json_encode([ 'index' => $index ]) . "\n";
        $_msearch_query .= json_encode([ 'size' => $vsize, 'query' => [ 'ids' => [ 'values' => $_id_values ] ] ]) . "\n";
        $count = $count + 1;

        unset($_id_values);
        $_id_values = [];

        if ($count < $qsize) {
            continue;
        }
        $count = 0;

        if ($parser->flag('verbose')) {
            fwrite(STDERR, sprintf('/%s/%s/_msearch', $index, $type) . PHP_EOL);
            fwrite(STDERR, $_msearch_query . PHP_EOL);
        }
        $response = $client->get(sprintf('/%s/%s/_msearch', $index, $type), ['body' => $_msearch_query]);

        echo $response->getBody(), PHP_EOL;

        $_msearch_query = '';
        sleep(1);
    }
    fclose($fp);

    if ($count > 0) {
        $_msearch_query .= json_encode([ 'index' => $index ]) . "\n";
        $_msearch_query .= json_encode([ 'size' => $vsize, 'query' => [ 'ids' => [ 'values' => $_id_values ] ] ]) . "\n";

        if ($parser->flag('verbose')) {
            fwrite(STDERR, sprintf('/%s/%s/_msearch', $index, $type) . PHP_EOL);
            fwrite(STDERR, $_msearch_query . PHP_EOL);
        }
        $response = $client->get(sprintf('/%s/%s/_msearch', $index, $type), ['body' => $_msearch_query]);

        echo $response->getBody(), PHP_EOL;
    }

} catch (\Exception $e) {
    throw $e;
}
