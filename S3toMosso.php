#!/usr/bin/php
<?php
/**
 * A PHP Script to transfer your objects from Amazon S3 to Rackspace CloudFiles (BTW, Mosso sounded nice)
 * To run it, fill in your S3 and Mosso Access credentials and invoke it from
 * the command line using something like
 *    user@host:~$ php5 path/to/S3toMosso.php
 *
 * Or if you make it executable (chmod+x) , you might simply run it directly
 *
 *
 *
 * The requirements are:
 *         php5             http://www.php.com
 *         Amazon S3 API    http://undesigned.org.za/2007/10/22/amazon-s3-php-class
 *              +php-S3 requirements
 *         cloudfiles API   http://github.com/rackspace/php-cloudfiles
 *              +cloudfiles API requirements
 *
 * @package     com.nefsystems.tools.php.s3tomosso
 * @author      NZEYIMANA Emery Fabrice <nzem@nefsystems.com>
 * @copyright   (c) 2009, NEFSYS. All rights reserved.
 * @license     Just change as you see fit. Let me know what your changes are if you want to share.
 * @todo        Setting Metadata
 * @todo        Check/Set public Bucket-->Container
 * @todo        Check errors like invalid KEYs, ...
 * @todo        Send a notification Email when the job is done
 * @todo        Add a flag to check if containers, objects don't exist already
 * @version     0.1.2
 */

// S3 settings
include_once('lib/S3.php');
$awsAccessID  = '------YOUR-AMAZON-AccessKeyID------';
$awsSecretKey = '----YOUR-AMAZON-SecretAccessKey----';
$awsExcludeBuckets = array(); // For example array('Bucket1', 'Bucket7', 'BucketN')
$awsExcludeObjects = array(); // For example array('Bucket3' => array('Object1', 'ObjectZ'), ...),
                              //                   'BucketK' => array('Object1', ...) )

// Mosso settings
include_once('lib/cloudfiles.php');
$mossoUsername = "----YOUR-MOSSO-USER-ID----";
$mossoAPIKey   = "----YOUR-MOSSO-API-KEY----";
$prefixToAddToContainers = ''; // Used only if you want to prepend anything to your new containers

@include_once('S3toMosso__private__.php'); // NEFSYS private accounts access. Remove if not NEFSYS

// Other settings
$directoryType = 'application/directory';

// Connect to S3
$objS3 = new S3($awsAccessID, $awsSecretKey);

// Connect to Mosso
$objMossoAuth = new CF_Authentication($mossoUsername, $mossoAPIKey);
$objMossoAuth->authenticate();
// Let's get a connection to CloudFiles
$objMosso = new CF_Connection($objMossoAuth);

echo "Listing buckets from your Amazon S3\n";
$awsBucketList = $objS3->listBuckets();
echo str_replace('Array', 'Amazon S3 Buckets', print_r($awsBucketList, true))."\n";

foreach ($awsBucketList as $awsBucketName)
{
    if (in_array($awsBucketName, $awsExcludeBuckets))
    {
        echo "---> Bucket $awsBucketName will be excluded\n";
        continue;
    }
    
    $mossoContainerName = $prefixToAddToContainers . $awsBucketName;
    // TODO: check if Bucket is CDN enabled
    // Get objects
    echo "Listing objects in Bucket $awsBucketName \n";
    $awsObjectList = $objS3->getBucket($awsBucketName);
    // Create this bucket as a Container on MOSSO
    echo "Creating Container $mossoContainerName in Cloud Files\n";
    $objMossoContainer = $objMosso->create_container($mossoContainerName);

    echo "Processing objects in Bucket $awsBucketName \n";

    foreach ($awsObjectList as $awsObjectInfo)
    {
        // Check if Object is in ignore list
        if (in_array($awsObjectInfo["name"], $awsExcludeObjects[$awsBucketName]))
        {
            echo "---> Object {$awsObjectInfo["name"]} will be excluded\n";
            continue;
        }

        //$awsObjectInfo = $objS3->getObjectInfo($awsBucketName, $awsObjectName);
        echo str_replace('Array', $awsObjectInfo["name"], print_r($awsObjectInfo, true));
        
        // TODO: Get Metadata and convert them to Mosso
        // Check if it's a folder
        if(strstr($awsObjectInfo["name"], '_$folder$'))
        {
            // No need to download anything, just create a folder entry
            $awsObjectInfo["name"] = substr($awsObjectInfo["name"], 0, -strlen('_$folder$'));
            echo 'Creating Marker for Folder ' . $awsObjectInfo["name"] . " on CloudFiles\n";
            $objMossoObject = $objMossoContainer->create_object($awsObjectInfo["name"]);
            $objMossoObject->content_type = $directoryType;// 'application/directory';
            
            // Get a Zero-byte file pointer
            $fp = @fopen("php://temp", "wb+");
            $objMossoObject->write($fp,0);
            @fclose($fp);
            continue;
        }

        // Get object into a TMP file
        $tmpFileName = tempnam(sys_get_temp_dir(), 'S3toMosso');
        
        echo 'Downloading ' . $awsObjectInfo["name"] . " from Amazon S3 to $tmpFileName\n";
        $objS3->getObject($awsBucketName, $awsObjectInfo["name"], $tmpFileName);
        
        // Send Object to Mosso
        echo 'Creating object ' . $awsObjectInfo["name"] . " in container $mossoContainerName\n";
        $objMossoObject  = $objMossoContainer->create_object($awsObjectInfo["name"]);
        
        try
        {
            $objMossoObject->_guess_content_type($tmpFileName);
        }
        catch (BadContentTypeException $e)
        {
            // Get the content type from Amazon
            $info = $objS3->getObjectInfo($awsBucketName, $awsObjectInfo["name"], true);
            $objMossoObject->content_type = $info['type'];
        }

        echo 'Uploading ' . $awsObjectInfo["name"] . ' to Cloud Files Server' . "\n";
        $objMossoObject->load_from_filename($tmpFileName);
        
        // Remove the TEMP file
        unlink($tmpFileName);
    }
}

////////////////////////////// THE END ///////////////////////////
?>