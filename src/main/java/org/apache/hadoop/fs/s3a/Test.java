package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Test {
    private final static String encryptionKeyBase64 = "g+7bgI4aOujRoccZxyi5CVsWrUgkwLzWYmiYcZKW0Gk=";

    // the iv must be generated using a secure random generator, this is hardcoded only for testing purposes
    private final static String ivBase64 = "PRrfBZze6v914JgV97V/IQ==";
    
    public static final String TRANSFORMATION = "AES/CTR/NoPadding";

    private static byte[] base64toBytes(String base64str) {
        return Base64.getDecoder().decode(base64str);
    }

    private static SecretKey base64toKey(String secretKeyBase64) {
        return new SecretKeySpec(Base64.getDecoder().decode(secretKeyBase64), "AES");
    }

    public static void main(String[] ars) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException, IOException {
        encryptSeekableIS("test.txt", "this is an example of plain text to be encrypted");
        decryptSeekableIS("test.txt");
    }

    private static void encryptSeekableIS(String encryptedFileName, String plainText) throws IOException {
        FileOutputStream encryptedOut = new FileOutputStream(encryptedFileName);
        Configuration conf = new Configuration();
        conf.set("aes.key", encryptionKeyBase64);
        conf.set("aes.iv", ivBase64);
        EncryptedS3AFileSystem fs = new EncryptedS3AFileSystem();
        fs.setConf(conf);

        FSDataOutputStream encryptedOutputStream = fs.encryptStream(new FSDataOutputStream(encryptedOut, null));
        encryptedOutputStream.write(plainText.getBytes());
    }

    private static void decryptSeekableIS(String encryptedFileName) throws NoSuchPaddingException,
            NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IOException {
        SecretKey encryptionKey = base64toKey(encryptionKeyBase64);
        byte[] iv = base64toBytes(ivBase64);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, new IvParameterSpec(iv));

        Path path = new Path(encryptedFileName);
        Configuration conf = new Configuration();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        FSDataInputStream in = new FSDataInputStream(inputStream);
        SeekableCipherInputStream seekableCipherInputStream = new SeekableCipherInputStream(in, encryptionKey, iv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] b = new byte[1024];
        int numberOfBytedRead;
        while ((numberOfBytedRead = seekableCipherInputStream.read(b)) >= 0) {
            baos.write(b, 0, numberOfBytedRead);
        }
        System.out.println(baos.toString());
        File outputFile = new File("decrypted_" + path.getName());
        Files.write(outputFile.toPath(), baos.toByteArray());
    }

    private static void ecnryptDecryptBytes() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
        SecretKey encryptionKey = base64toKey(encryptionKeyBase64);
        byte[] iv = base64toBytes(ivBase64);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.ENCRYPT_MODE, encryptionKey, new IvParameterSpec(iv));
        byte[] plainTextBytes = "Hello World".getBytes();
        byte[] encryptedBytes = cipher.update(plainTextBytes);

        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, new IvParameterSpec(iv));
        byte[] decryptedBytes = cipher.update(encryptedBytes);

        System.out.println(new String(decryptedBytes));
    }

    private static void decryptFileIS(String fileName) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IOException {
        SecretKey encryptionKey = base64toKey(encryptionKeyBase64);
        byte[] iv = base64toBytes(ivBase64);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, new IvParameterSpec(iv));

        FileInputStream fis = new FileInputStream(fileName);
        CipherInputStream in = new CipherInputStream(fis, cipher);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] b = new byte[1024];
        int numberOfBytedRead;
        while ((numberOfBytedRead = in.read(b)) >= 0) {
            baos.write(b, 0, numberOfBytedRead);
        }
        System.out.println(new String(baos.toByteArray()));
    }

    private static void decryptFileAsBytes(String name) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IOException {
        SecretKey encryptionKey = base64toKey(encryptionKeyBase64);
        byte[] iv = base64toBytes(ivBase64);

        Cipher cipher = Cipher.getInstance(TRANSFORMATION);
        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, new IvParameterSpec(iv));
        File encryptedFile = new File(name);
        byte[] encryptedBytes = Files.readAllBytes(encryptedFile.toPath());
        byte[] decryptedBytes = cipher.update(encryptedBytes);
        System.out.println(new String(decryptedBytes, StandardCharsets.UTF_8));

        File outputFile = new File("decrypted_" + encryptedFile.getName());
        Files.write(outputFile.toPath(), decryptedBytes);
    }
}
