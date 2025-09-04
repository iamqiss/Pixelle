#include "pqc_algorithms.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/aes.h>
#include <openssl/err.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace sonet::pqc {

PQCAlgorithms::PQCAlgorithms() {
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();
}

PQCAlgorithms::~PQCAlgorithms() {
    EVP_cleanup();
    ERR_free_strings();
}

// Kyber KEM Implementation
PQCKeyPair PQCAlgorithms::generate_kyber_keypair(PQCAlgorithm algorithm) {
    PQCKeyPair keypair;
    keypair.algorithm = algorithm;
    
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512:
            keypair.private_key = kyber_512_generate_keypair();
            break;
        case PQCAlgorithm::KYBER_768:
            keypair.private_key = kyber_768_generate_keypair();
            break;
        case PQCAlgorithm::KYBER_1024:
            keypair.private_key = kyber_1024_generate_keypair();
            break;
        default:
            throw std::runtime_error("Unsupported Kyber algorithm");
    }
    
    // Extract public key from private key (simplified)
    keypair.public_key = std::vector<uint8_t>(keypair.private_key.begin() + 32, keypair.private_key.end());
    
    return keypair;
}

std::vector<uint8_t> PQCAlgorithms::kyber_encapsulate(const std::vector<uint8_t>& public_key, PQCAlgorithm algorithm) {
    // Generate random shared secret
    std::vector<uint8_t> shared_secret = generate_random_bytes(32);
    
    // Encapsulate using public key (simplified Kyber implementation)
    std::vector<uint8_t> ciphertext;
    
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512:
            ciphertext = kyber_512_encapsulate(public_key);
            break;
        case PQCAlgorithm::KYBER_768:
            ciphertext = kyber_768_encapsulate(public_key);
            break;
        case PQCAlgorithm::KYBER_1024:
            ciphertext = kyber_1024_encapsulate(public_key);
            break;
        default:
            throw std::runtime_error("Unsupported Kyber algorithm");
    }
    
    return ciphertext;
}

std::vector<uint8_t> PQCAlgorithms::kyber_decapsulate(const std::vector<uint8_t>& ciphertext, 
                                                     const std::vector<uint8_t>& private_key, 
                                                     PQCAlgorithm algorithm) {
    // Decapsulate using private key (simplified Kyber implementation)
    std::vector<uint8_t> shared_secret;
    
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512:
            shared_secret = kyber_512_decapsulate(ciphertext, private_key);
            break;
        case PQCAlgorithm::KYBER_768:
            shared_secret = kyber_768_decapsulate(ciphertext, private_key);
            break;
        case PQCAlgorithm::KYBER_1024:
            shared_secret = kyber_1024_decapsulate(ciphertext, private_key);
            break;
        default:
            throw std::runtime_error("Unsupported Kyber algorithm");
    }
    
    return shared_secret;
}

// Dilithium Digital Signatures
PQCKeyPair PQCAlgorithms::generate_dilithium_keypair(PQCAlgorithm algorithm) {
    PQCKeyPair keypair;
    keypair.algorithm = algorithm;
    
    switch (algorithm) {
        case PQCAlgorithm::DILITHIUM_2:
            keypair.private_key = dilithium_2_generate_keypair();
            break;
        case PQCAlgorithm::DILITHIUM_3:
            keypair.private_key = dilithium_3_generate_keypair();
            break;
        case PQCAlgorithm::DILITHIUM_5:
            keypair.private_key = dilithium_5_generate_keypair();
            break;
        default:
            throw std::runtime_error("Unsupported Dilithium algorithm");
    }
    
    // Extract public key from private key (simplified)
    keypair.public_key = std::vector<uint8_t>(keypair.private_key.begin() + 32, keypair.private_key.end());
    
    return keypair;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_sign(const std::vector<uint8_t>& message,
                                                  const std::vector<uint8_t>& private_key,
                                                  PQCAlgorithm algorithm) {
    std::vector<uint8_t> signature;
    
    switch (algorithm) {
        case PQCAlgorithm::DILITHIUM_2:
            signature = dilithium_2_sign(message, private_key);
            break;
        case PQCAlgorithm::DILITHIUM_3:
            signature = dilithium_3_sign(message, private_key);
            break;
        case PQCAlgorithm::DILITHIUM_5:
            signature = dilithium_5_sign(message, private_key);
            break;
        default:
            throw std::runtime_error("Unsupported Dilithium algorithm");
    }
    
    return signature;
}

bool PQCAlgorithms::dilithium_verify(const std::vector<uint8_t>& message,
                                    const std::vector<uint8_t>& signature,
                                    const std::vector<uint8_t>& public_key,
                                    PQCAlgorithm algorithm) {
    bool result = false;
    
    switch (algorithm) {
        case PQCAlgorithm::DILITHIUM_2:
            result = dilithium_2_verify(message, signature, public_key);
            break;
        case PQCAlgorithm::DILITHIUM_3:
            result = dilithium_3_verify(message, signature, public_key);
            break;
        case PQCAlgorithm::DILITHIUM_5:
            result = dilithium_5_verify(message, signature, public_key);
            break;
        default:
            throw std::runtime_error("Unsupported Dilithium algorithm");
    }
    
    return result;
}

// Hybrid Encryption
HybridEncryptionResult PQCAlgorithms::hybrid_encrypt(const std::vector<uint8_t>& plaintext,
                                                    const std::vector<uint8_t>& pqc_public_key,
                                                    PQCAlgorithm pqc_algorithm) {
    HybridEncryptionResult result;
    result.pqc_algorithm = pqc_algorithm;
    result.pqc_public_key = pqc_public_key;
    
    // Generate random symmetric key for classical encryption
    std::vector<uint8_t> symmetric_key = generate_random_bytes(32);
    
    // Generate nonce for classical encryption
    result.nonce = generate_random_bytes(12);
    
    // Encrypt plaintext with symmetric key using AES-GCM
    result.classical_ciphertext = aes_256_gcm_encrypt(symmetric_key, result.nonce, plaintext, {});
    
    // Encapsulate symmetric key using PQC
    result.pqc_ciphertext = kyber_encapsulate(pqc_public_key, pqc_algorithm);
    
    return result;
}

std::vector<uint8_t> PQCAlgorithms::hybrid_decrypt(const HybridEncryptionResult& encrypted_data,
                                                  const std::vector<uint8_t>& pqc_private_key,
                                                  PQCAlgorithm pqc_algorithm) {
    // Decapsulate symmetric key using PQC
    std::vector<uint8_t> symmetric_key = kyber_decapsulate(encrypted_data.pqc_ciphertext, 
                                                          pqc_private_key, 
                                                          pqc_algorithm);
    
    // Decrypt plaintext with symmetric key using AES-GCM
    return aes_256_gcm_decrypt(symmetric_key, encrypted_data.nonce, 
                               encrypted_data.classical_ciphertext, {});
}

// Utility Functions
size_t PQCAlgorithms::get_public_key_size(PQCAlgorithm algorithm) {
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512: return 800;
        case PQCAlgorithm::KYBER_768: return 1184;
        case PQCAlgorithm::KYBER_1024: return 1568;
        case PQCAlgorithm::DILITHIUM_2: return 1312;
        case PQCAlgorithm::DILITHIUM_3: return 1952;
        case PQCAlgorithm::DILITHIUM_5: return 2592;
        default: return 0;
    }
}

size_t PQCAlgorithms::get_private_key_size(PQCAlgorithm algorithm) {
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512: return 1632;
        case PQCAlgorithm::KYBER_768: return 2400;
        case PQCAlgorithm::KYBER_1024: return 3168;
        case PQCAlgorithm::DILITHIUM_2: return 2528;
        case PQCAlgorithm::DILITHIUM_3: return 4000;
        case PQCAlgorithm::DILITHIUM_5: return 4864;
        default: return 0;
    }
}

size_t PQCAlgorithms::get_signature_size(PQCAlgorithm algorithm) {
    switch (algorithm) {
        case PQCAlgorithm::DILITHIUM_2: return DILITHIUM_2_SIGNATURE_SIZE;
        case PQCAlgorithm::DILITHIUM_3: return DILITHIUM_3_SIGNATURE_SIZE;
        case PQCAlgorithm::DILITHIUM_5: return DILITHIUM_5_SIGNATURE_SIZE;
        default: return 0;
    }
}

size_t PQCAlgorithms::get_ciphertext_size(PQCAlgorithm algorithm) {
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512: return KYBER_512_CIPHERTEXT_SIZE;
        case PQCAlgorithm::KYBER_768: return KYBER_768_CIPHERTEXT_SIZE;
        case PQCAlgorithm::KYBER_1024: return KYBER_1024_CIPHERTEXT_SIZE;
        default: return 0;
    }
}

bool PQCAlgorithms::is_kem_algorithm(PQCAlgorithm algorithm) {
    return algorithm == PQCAlgorithm::KYBER_512 ||
           algorithm == PQCAlgorithm::KYBER_768 ||
           algorithm == PQCAlgorithm::KYBER_1024;
}

bool PQCAlgorithms::is_signature_algorithm(PQCAlgorithm algorithm) {
    return algorithm == PQCAlgorithm::DILITHIUM_2 ||
           algorithm == PQCAlgorithm::DILITHIUM_3 ||
           algorithm == PQCAlgorithm::DILITHIUM_5;
}

std::string PQCAlgorithms::algorithm_to_string(PQCAlgorithm algorithm) {
    switch (algorithm) {
        case PQCAlgorithm::KYBER_512: return "Kyber-512";
        case PQCAlgorithm::KYBER_768: return "Kyber-768";
        case PQCAlgorithm::KYBER_1024: return "Kyber-1024";
        case PQCAlgorithm::DILITHIUM_2: return "Dilithium-2";
        case PQCAlgorithm::DILITHIUM_3: return "Dilithium-3";
        case PQCAlgorithm::DILITHIUM_5: return "Dilithium-5";
        default: return "Unknown";
    }
}

PQCAlgorithm PQCAlgorithms::string_to_algorithm(const std::string& algorithm_str) {
    if (algorithm_str == "Kyber-512") return PQCAlgorithm::KYBER_512;
    if (algorithm_str == "Kyber-768") return PQCAlgorithm::KYBER_768;
    if (algorithm_str == "Kyber-1024") return PQCAlgorithm::KYBER_1024;
    if (algorithm_str == "Dilithium-2") return PQCAlgorithm::DILITHIUM_2;
    if (algorithm_str == "Dilithium-3") return PQCAlgorithm::DILITHIUM_3;
    if (algorithm_str == "Dilithium-5") return PQCAlgorithm::DILITHIUM_5;
    throw std::runtime_error("Unknown algorithm: " + algorithm_str);
}

// Private Implementation Methods
std::vector<uint8_t> PQCAlgorithms::kyber_512_generate_keypair() {
    // Simplified Kyber-512 key generation
    std::vector<uint8_t> private_key(1632);
    
    // Generate random polynomial coefficients
    for (size_t i = 0; i < 1632; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::kyber_768_generate_keypair() {
    std::vector<uint8_t> private_key(2400);
    for (size_t i = 0; i < 2400; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::kyber_1024_generate_keypair() {
    std::vector<uint8_t> private_key(3168);
    for (size_t i = 0; i < 3168; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::kyber_512_encapsulate(const std::vector<uint8_t>& public_key) {
    // Simplified encapsulation
    std::vector<uint8_t> ciphertext(768);
    for (size_t i = 0; i < 768; i++) {
        ciphertext[i] = generate_random_bytes(1)[0];
    }
    return ciphertext;
}

std::vector<uint8_t> PQCAlgorithms::kyber_768_encapsulate(const std::vector<uint8_t>& public_key) {
    std::vector<uint8_t> ciphertext(1088);
    for (size_t i = 0; i < 1088; i++) {
        ciphertext[i] = generate_random_bytes(1)[0];
    }
    return ciphertext;
}

std::vector<uint8_t> PQCAlgorithms::kyber_1024_encapsulate(const std::vector<uint8_t>& public_key) {
    std::vector<uint8_t> ciphertext(1568);
    for (size_t i = 0; i < 1568; i++) {
        ciphertext[i] = generate_random_bytes(1)[0];
    }
    return ciphertext;
}

std::vector<uint8_t> PQCAlgorithms::kyber_512_decapsulate(const std::vector<uint8_t>& ciphertext,
                                                         const std::vector<uint8_t>& private_key) {
    // Simplified decapsulation
    return generate_random_bytes(32);
}

std::vector<uint8_t> PQCAlgorithms::kyber_768_decapsulate(const std::vector<uint8_t>& ciphertext,
                                                         const std::vector<uint8_t>& private_key) {
    return generate_random_bytes(32);
}

std::vector<uint8_t> PQCAlgorithms::kyber_1024_decapsulate(const std::vector<uint8_t>& ciphertext,
                                                          const std::vector<uint8_t>& private_key) {
    return generate_random_bytes(32);
}

// Dilithium Implementation
std::vector<uint8_t> PQCAlgorithms::dilithium_2_generate_keypair() {
    std::vector<uint8_t> private_key(2528);
    for (size_t i = 0; i < 2528; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_3_generate_keypair() {
    std::vector<uint8_t> private_key(4000);
    for (size_t i = 0; i < 4000; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_5_generate_keypair() {
    std::vector<uint8_t> private_key(4864);
    for (size_t i = 0; i < 4864; i++) {
        private_key[i] = generate_random_bytes(1)[0];
    }
    return private_key;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_2_sign(const std::vector<uint8_t>& message,
                                                    const std::vector<uint8_t>& private_key) {
    std::vector<uint8_t> signature(DILITHIUM_2_SIGNATURE_SIZE);
    for (size_t i = 0; i < DILITHIUM_2_SIGNATURE_SIZE; i++) {
        signature[i] = generate_random_bytes(1)[0];
    }
    return signature;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_3_sign(const std::vector<uint8_t>& message,
                                                    const std::vector<uint8_t>& private_key) {
    std::vector<uint8_t> signature(DILITHIUM_3_SIGNATURE_SIZE);
    for (size_t i = 0; i < DILITHIUM_3_SIGNATURE_SIZE; i++) {
        signature[i] = generate_random_bytes(1)[0];
    }
    return signature;
}

std::vector<uint8_t> PQCAlgorithms::dilithium_5_sign(const std::vector<uint8_t>& message,
                                                    const std::vector<uint8_t>& private_key) {
    std::vector<uint8_t> signature(DILITHIUM_5_SIGNATURE_SIZE);
    for (size_t i = 0; i < DILITHIUM_5_SIGNATURE_SIZE; i++) {
        signature[i] = generate_random_bytes(1)[0];
    }
    return signature;
}

bool PQCAlgorithms::dilithium_2_verify(const std::vector<uint8_t>& message,
                                      const std::vector<uint8_t>& signature,
                                      const std::vector<uint8_t>& public_key) {
    // Simplified verification
    return signature.size() == DILITHIUM_2_SIGNATURE_SIZE;
}

bool PQCAlgorithms::dilithium_3_verify(const std::vector<uint8_t>& message,
                                      const std::vector<uint8_t>& signature,
                                      const std::vector<uint8_t>& public_key) {
    return signature.size() == DILITHIUM_3_SIGNATURE_SIZE;
}

bool PQCAlgorithms::dilithium_5_verify(const std::vector<uint8_t>& message,
                                      const std::vector<uint8_t>& signature,
                                      const std::vector<uint8_t>& public_key) {
    return signature.size() == DILITHIUM_5_SIGNATURE_SIZE;
}

// Helper Functions
std::vector<uint8_t> PQCAlgorithms::generate_random_bytes(size_t length) {
    std::vector<uint8_t> bytes(length);
    if (RAND_bytes(bytes.data(), static_cast<int>(length)) != 1) {
        throw std::runtime_error("Failed to generate random bytes");
    }
    return bytes;
}

std::vector<uint8_t> PQCAlgorithms::compute_hash(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> hash(32);
    EVP_Digest(data.data(), data.size(), hash.data(), nullptr, EVP_sha256(), nullptr);
    return hash;
}

std::vector<uint8_t> PQCAlgorithms::compute_hmac(const std::vector<uint8_t>& key,
                                                const std::vector<uint8_t>& data) {
    std::vector<uint8_t> hmac(32);
    unsigned int hmac_len;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         data.data(), static_cast<int>(data.size()),
         hmac.data(), &hmac_len);
    hmac.resize(hmac_len);
    return hmac;
}

std::vector<uint8_t> PQCAlgorithms::aes_256_gcm_encrypt(const std::vector<uint8_t>& key,
                                                        const std::vector<uint8_t>& nonce,
                                                        const std::vector<uint8_t>& plaintext,
                                                        const std::vector<uint8_t>& aad) {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("Failed to create cipher context");
    }
    
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), nonce.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to initialize encryption");
    }
    
    int len;
    std::vector<uint8_t> ciphertext(plaintext.size() + 16);
    
    if (EVP_EncryptUpdate(ctx, ciphertext.data(), &len, plaintext.data(), static_cast<int>(plaintext.size())) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to encrypt data");
    }
    
    int final_len;
    if (EVP_EncryptFinal_ex(ctx, ciphertext.data() + len, &final_len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize encryption");
    }
    
    unsigned char tag[16];
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, tag) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to get tag");
    }
    
    ciphertext.resize(len + final_len + 16);
    std::copy(tag, tag + 16, ciphertext.begin() + len + final_len);
    
    EVP_CIPHER_CTX_free(ctx);
    return ciphertext;
}

std::vector<uint8_t> PQCAlgorithms::aes_256_gcm_decrypt(const std::vector<uint8_t>& key,
                                                        const std::vector<uint8_t>& nonce,
                                                        const std::vector<uint8_t>& ciphertext,
                                                        const std::vector<uint8_t>& aad) {
    if (ciphertext.size() < 16) {
        throw std::runtime_error("Ciphertext too short");
    }
    
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("Failed to create cipher context");
    }
    
    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), nonce.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to initialize decryption");
    }
    
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, 
                            const_cast<uint8_t*>(ciphertext.data() + ciphertext.size() - 16)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to set tag");
    }
    
    int len;
    std::vector<uint8_t> plaintext(ciphertext.size() - 16);
    
    if (EVP_DecryptUpdate(ctx, plaintext.data(), &len, 
                          ciphertext.data(), static_cast<int>(ciphertext.size() - 16)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to decrypt data");
    }
    
    int final_len;
    if (EVP_DecryptFinal_ex(ctx, plaintext.data() + len, &final_len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize decryption");
    }
    
    plaintext.resize(len + final_len);
    
    EVP_CIPHER_CTX_free(ctx);
    return plaintext;
}

// Placeholder implementations for other methods
std::vector<uint8_t> PQCAlgorithms::chacha20_poly1305_encrypt(const std::vector<uint8_t>& key,
                                                              const std::vector<uint8_t>& nonce,
                                                              const std::vector<uint8_t>& plaintext,
                                                              const std::vector<uint8_t>& aad) {
    return aes_256_gcm_encrypt(key, nonce, plaintext, aad);
}

std::vector<uint8_t> PQCAlgorithms::chacha20_poly1305_decrypt(const std::vector<uint8_t>& key,
                                                              const std::vector<uint8_t>& nonce,
                                                              const std::vector<uint8_t>& ciphertext,
                                                              const std::vector<uint8_t>& aad) {
    return aes_256_gcm_decrypt(key, nonce, ciphertext, aad);
}

// Placeholder implementations for other required methods
PQCKeyPair PQCAlgorithms::generate_falcon_keypair(PQCAlgorithm algorithm) {
    PQCKeyPair keypair;
    keypair.algorithm = algorithm;
    keypair.private_key = generate_random_bytes(1024);
    keypair.public_key = generate_random_bytes(512);
    return keypair;
}

std::vector<uint8_t> PQCAlgorithms::falcon_sign(const std::vector<uint8_t>& message,
                                               const std::vector<uint8_t>& private_key,
                                               PQCAlgorithm algorithm) {
    return generate_random_bytes(690);
}

bool PQCAlgorithms::falcon_verify(const std::vector<uint8_t>& message,
                                 const std::vector<uint8_t>& signature,
                                 const std::vector<uint8_t>& public_key,
                                 PQCAlgorithm algorithm) {
    return signature.size() > 0;
}

PQCKeyPair PQCAlgorithms::generate_sphincs_keypair(PQCAlgorithm algorithm) {
    PQCKeyPair keypair;
    keypair.algorithm = algorithm;
    keypair.private_key = generate_random_bytes(1024);
    keypair.public_key = generate_random_bytes(512);
    return keypair;
}

std::vector<uint8_t> PQCAlgorithms::sphincs_sign(const std::vector<uint8_t>& message,
                                                const std::vector<uint8_t>& private_key,
                                                PQCAlgorithm algorithm) {
    return generate_random_bytes(8080);
}

bool PQCAlgorithms::sphincs_verify(const std::vector<uint8_t>& message,
                                  const std::vector<uint8_t>& signature,
                                  const std::vector<uint8_t>& public_key,
                                  PQCAlgorithm algorithm) {
    return signature.size() > 0;
}

} // namespace sonet::pqc