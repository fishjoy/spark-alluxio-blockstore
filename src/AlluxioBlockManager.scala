/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, Random}

import scala.util.control.NonFatal
import com.google.common.io.ByteStreams
import alluxio.{AlluxioURI, Constants}
import alluxio.client.ClientContext
import alluxio.client.file.{FileOutStream, FileSystem}
import alluxio.client.file.FileSystem.Factory
import alluxio.client.file.options.{CreateDirectoryOptions, DeleteOptions}
import alluxio.exception.{FileAlreadyExistsException, FileDoesNotExistException}
import org.apache.spark.Logging
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils


/**
 * Creates and maintains the logical mapping between logical blocks and alluxio fs locations. By
 * default, one block is mapped to one file with a name given by its BlockId.
 *
 */
private[spark] class AlluxioBlockManager() extends ExternalBlockManager with Logging {

  var rootDirs: String = _
  var master: String = _
  var client: FileSystem = _
  private var subDirsPerAlluxioDir: Int = _

  // Create one Alluxio directory for each path mentioned in spark.alluxioStore.folderName;
  // then, inside this directory, create multiple subdirectories that we will hash files into,
  // in order to avoid having really large inodes at the top level in Alluxio.
  private var alluxioDirs: Array[AlluxioURI] = _
  private var subDirs: Array[Array[AlluxioURI]] = _
  private val shutdownDeleteAlluxioPaths = new scala.collection.mutable.HashSet[String]()

  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)
    val storeDir = blockManager.conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_alluxio")
    val appFolderName = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)

    rootDirs = s"$storeDir/$appFolderName/$executorId"
    master = blockManager.conf.get(ExternalBlockStore.MASTER_URL, "alluxio://localhost:19998")
    val value = master.replace("/", "").split(":")
    client = if (master != null && master != "") {
      ClientContext.getConf.set(Constants.MASTER_HOSTNAME, value(1))
      ClientContext.init()
      Factory.get()
    } else {
      null
    }
    // original implementation call System.exit, we change it to run without extblkstore support
    if (client == null) {
      logError("Failed to connect to the Alluxio as the master address is not configured")
      throw new IOException("Failed to connect to the Alluxio as the master " +
        "address is not configured")
    }
    subDirsPerAlluxioDir = blockManager.conf.get("spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR).toInt

    // Create one Alluxio directory for each path mentioned in spark.alluxioStore.folderName;
    // then, inside this directory, create multiple subdirectories that we will hash files into,
    // in order to avoid having really large inodes at the top level in Alluxio.
    alluxioDirs = createAlluxioDirs()
    subDirs = Array.fill(alluxioDirs.length)(new Array[AlluxioURI](subDirsPerAlluxioDir))
    alluxioDirs.foreach(registerShutdownDeleteDir)
  }

  override def toString: String = {"ExternalBlockStore-Alluxio"}

  override def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (fileExists(file)) {
      removeFile(file)
      true
    } else {
      false
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    fileExists(file)
  }


  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = getFile(blockId)
    var os: FileOutStream = null
    try {
      os = client.createFile(file)
      writeByteBuffer(bytes, os)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put bytes of block $blockId into Alluxio", e)
        os.cancel()
      case _: FileAlreadyExistsException => file
    } finally {
      os.close()
    }
  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val file = getFile(blockId)
    var os: FileOutStream = null
    try {
      os = client.createFile(file)
      blockManager.dataSerializeStream(blockId, os, values)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Alluxio", e)
        os.cancel()
      case _: FileAlreadyExistsException => file
    } finally {
      os.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null) {
      return None
    }
    val is = try {
      client.openFile(file)
    } catch {
      case _: FileDoesNotExistException =>
        return None
    }
    try {
      val size = getSize(blockId)
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from Alluxio", e)
        None
    } finally {
      is.close()
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val file = getFile(blockId)
    if (file == null) {
      return None
    }
    val is = try {
      client.openFile(file)
    } catch {
      case _: FileDoesNotExistException =>
        return None
    }
    try {
      Some(blockManager.dataDeserializeStream(blockId, is))
    } finally {
      is.close()
    }
  }

  override def getSize(blockId: BlockId): Long = {
    client.getStatus(getFile(blockId)).getLength
  }

  def removeFile(file: AlluxioURI): Unit = {
    client.delete(file)
  }

  def fileExists(file: AlluxioURI): Boolean = {
    try {
      client.exists(file)
    } catch {
      case _: FileDoesNotExistException => false
    }
  }

  def getFile(filename: String): AlluxioURI = {
    // Figure out which alluxio directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % alluxioDirs.length
    val subDirId = (hash / alluxioDirs.length) % subDirsPerAlluxioDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = new AlluxioURI(s"${alluxioDirs(dirId)}/${"%02x".format(subDirId)}")
          client.createDirectory(path, CreateDirectoryOptions.defaults().setRecursive(true))
          subDirs(dirId)(subDirId) = path
          path
        }
      }
    }
    new AlluxioURI(s"$subDir/$filename")
  }

  def getFile(blockId: BlockId): AlluxioURI = getFile(blockId.name)

  // TODO: Some of the logic here could be consolidated/de-duplicated with that in the DiskStore.
  private def createAlluxioDirs(): Array[AlluxioURI] = {
    logDebug("Creating alluxio directories at root dirs '" + rootDirs + "'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var alluxioDir: AlluxioURI = null
      var alluxioDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          alluxioDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          val path = new AlluxioURI(s"$rootDir/spark-alluxio-$alluxioDirId")
          try {
            client.createDirectory(path, CreateDirectoryOptions.defaults().setRecursive(true))
            alluxioDir = path
            foundLocalDir = true
          } catch {
            case _: FileAlreadyExistsException => // continue
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Attempt " + tries + " to create alluxio dir " + alluxioDir + " failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS
          + " attempts to create alluxio dir in " + rootDir)
        System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created alluxio directory at " + alluxioDir)
      alluxioDir
    }
  }

  override def shutdown() {
    logDebug("Shutdown hook called")
    alluxioDirs.foreach { alluxioDir =>
      try {
        if (!hasRootAsShutdownDeleteDir(alluxioDir)) {
          deleteRecursively(alluxioDir, client)
        }
      } catch {
        case NonFatal(e) =>
          logError("Exception while deleting alluxio spark dir: " + alluxioDir, e)
      }
    }
  }

  /**
    * Delete a file or directory and its contents recursively.
    */
  private def deleteRecursively(dir: AlluxioURI, client: FileSystem) {
    client.delete(dir, DeleteOptions.defaults().setRecursive(true))
  }

  // Register the alluxio path to be deleted via shutdown hook
  private def registerShutdownDeleteDir(file: AlluxioURI) {
    val absolutePath = file.toString
    shutdownDeleteAlluxioPaths.synchronized {
      shutdownDeleteAlluxioPaths += absolutePath
    }
  }

  // Remove the alluxio path to be deleted via shutdown hook
  private def removeShutdownDeleteDir(file: AlluxioURI) {
    val absolutePath = file.toString
    shutdownDeleteAlluxioPaths.synchronized {
      shutdownDeleteAlluxioPaths -= absolutePath
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  private def hasShutdownDeleteAlluxioDir(file: AlluxioURI): Boolean = {
    val absolutePath = file.toString
    shutdownDeleteAlluxioPaths.synchronized {
      shutdownDeleteAlluxioPaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in Exception and incomplete cleanup.
  private def hasRootAsShutdownDeleteDir(file: AlluxioURI): Boolean = {
    val absolutePath = file.toString
    val hasRoot = shutdownDeleteAlluxioPaths.synchronized {
      shutdownDeleteAlluxioPaths.exists(
        path => !absolutePath.equals(path) && absolutePath.startsWith(path))
    }
    if (hasRoot) {
      logInfo(s"path = $absolutePath, already present as root for deletion.")
    }
    hasRoot
  }

}
