#pragma once

#include <vector>
#include <string>
#include <memory>
#include <cstdint>
#include <optional>
#include <array>

namespace sonet::pqc {

// PQC Algorithm Constants
constexpr size_t KYBER_512_KEY_SIZE = 800;
constexpr size_t KYBER_512_CIPHERTEXT_SIZE = 768;
constexpr size_t KYBER_768_KEY_SIZE = 1184;
constexpr size_t KYBER_768_CIPHERTEXT_SIZE = 1088;
constexpr size_t KYBER_1024_KEY_SIZE = 1568;
constexpr size_t KYBER_1024_CIPHERTEXT_SIZE = 1568;

constexpr size_t DILITHIUM_2_SIGNATURE_SIZE = 2701;
constexpr size_t DILITHIUM_3_SIGNATURE_SIZE = 3366;
constexpr size_t DILITHIUM_5_SIGNATURE_SIZE = 4595;

constexpr size_t FALCON_512_SIGNATURE_SIZE = 690;
constexpr size_t FALCON_1024_SIGNATURE_SIZE = 1330;

constexpr size_t SPHINCS_SHA256_128F_SIMPLE_SIGNATURE_SIZE = 8080;
constexpr size_t SPHINCS_SHA256_192F_SIMPLE_SIGNATURE_SIZE = 16224;
constexpr size_t SPHINCS_SHA256_256F_SIMPLE_SIGNATURE_SIZE = 49216;

// PQC Algorithm Types
enum class PQCAlgorithm : uint8_t {
    KYBER_512 = 0x01,
    KYBER_768 = 0x02,
    KYBER_1024 = 0x03,
    DILITHIUM_2 = 0x04,
    DILITHIUM_3 = 0x05,
    DILITHIUM_5 = 0x06,
    FALCON_512 = 0x07,
    FALCON_1024 = 0x08,
    SPHINCS_SHA256_128F_SIMPLE = 0x09,
    SPHINCS_SHA256_192F_SIMPLE = 0x0A,
    SPHINCS_SHA256_256F_SIMPLE = 0x0B
};

// PQC Key Pair
struct PQCKeyPair {
    std::vector<uint8_t> public_key;
    std::vector<uint8_t> private_key;
    PQCAlgorithm algorithm;
};

// PQC Signature
struct PQCSignature {
    std::vector<uint8_t> signature;
    PQCAlgorithm algorithm;
    std::vector<uint8_t> public_key;
};

// PQC Ciphertext
struct PQCCiphertext {
    std::vector<uint8_t> ciphertext;
    PQCAlgorithm algorithm;
    std::vector<uint8_t> public_key;
};

// Hybrid Encryption Result
struct HybridEncryptionResult {
    std::vector<uint8_t> classical_ciphertext;  // AES/ChaCha20 encrypted data
    std::vector<uint8_t> pqc_ciphertext;       // PQC encrypted key
    std::vector<uint8_t> nonce;                // Nonce for classical encryption
    PQCAlgorithm pqc_algorithm;                // PQC algorithm used
    std::vector<uint8_t> pqc_public_key;       // PQC public key used
};

// PQC Algorithms Implementation
class PQCAlgorithms {
public:
    PQCAlgorithms();
    ~PQCAlgorithms();

    // Kyber KEM (Key Encapsulation Mechanism)
    PQCKeyPair generate_kyber_keypair(PQCAlgorithm algorithm);
    std::vector<uint8_t> kyber_encapsulate(const std::vector<uint8_t>& public_key, PQCAlgorithm algorithm);
    std::vector<uint8_t> kyber_decapsulate(const std::vector<uint8_t>& ciphertext, 
                                          const std::vector<uint8_t>& private_key, 
                                          PQCAlgorithm algorithm);

    // Dilithium Digital Signatures
    PQCKeyPair generate_dilithium_keypair(PQCAlgorithm algorithm);
    std::vector<uint8_t> dilithium_sign(const std::vector<uint8_t>& message,
                                       const std::vector<uint8_t>& private_key,
                                       PQCAlgorithm algorithm);
    bool dilithium_verify(const std::vector<uint8_t>& message,
                         const std::vector<uint8_t>& signature,
                         const std::vector<uint8_t>& public_key,
                         PQCAlgorithm algorithm);

    // Falcon Digital Signatures
    PQCKeyPair generate_falcon_keypair(PQCAlgorithm algorithm);
    std::vector<uint8_t> falcon_sign(const std::vector<uint8_t>& message,
                                    const std::vector<uint8_t>& private_key,
                                    PQCAlgorithm algorithm);
    bool falcon_verify(const std::vector<uint8_t>& message,
                      const std::vector<uint8_t>& signature,
                      const std::vector<uint8_t>& public_key,
                      PQCAlgorithm algorithm);

    // SPHINCS+ Digital Signatures
    PQCKeyPair generate_sphincs_keypair(PQCAlgorithm algorithm);
    std::vector<uint8_t> sphincs_sign(const std::vector<uint8_t>& message,
                                     const std::vector<uint8_t>& private_key,
                                     PQCAlgorithm algorithm);
    bool sphincs_verify(const std::vector<uint8_t>& message,
                       const std::vector<uint8_t>& signature,
                       const std::vector<uint8_t>& public_key,
                       PQCAlgorithm algorithm);

    // Hybrid Encryption/Decryption
    HybridEncryptionResult hybrid_encrypt(const std::vector<uint8_t>& plaintext,
                                         const std::vector<uint8_t>& pqc_public_key,
                                         PQCAlgorithm pqc_algorithm);
    
    std::vector<uint8_t> hybrid_decrypt(const HybridEncryptionResult& encrypted_data,
                                       const std::vector<uint8_t>& pqc_private_key,
                                       PQCAlgorithm pqc_algorithm);

    // Utility Functions
    size_t get_public_key_size(PQCAlgorithm algorithm);
    size_t get_private_key_size(PQCAlgorithm algorithm);
    size_t get_signature_size(PQCAlgorithm algorithm);
    size_t get_ciphertext_size(PQCAlgorithm algorithm);
    
    bool is_kem_algorithm(PQCAlgorithm algorithm);
    bool is_signature_algorithm(PQCAlgorithm algorithm);
    
    std::string algorithm_to_string(PQCAlgorithm algorithm);
    PQCAlgorithm string_to_algorithm(const std::string& algorithm_str);

private:
    // Kyber Implementation
    std::vector<uint8_t> kyber_512_generate_keypair();
    std::vector<uint8_t> kyber_768_generate_keypair();
    std::vector<uint8_t> kyber_1024_generate_keypair();
    
    std::vector<uint8_t> kyber_512_encapsulate(const std::vector<uint8_t>& public_key);
    std::vector<uint8_t> kyber_768_encapsulate(const std::vector<uint8_t>& public_key);
    std::vector<uint8_t> kyber_1024_encapsulate(const std::vector<uint8_t>& public_key);
    
    std::vector<uint8_t> kyber_512_decapsulate(const std::vector<uint8_t>& ciphertext,
                                              const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> kyber_768_decapsulate(const std::vector<uint8_t>& ciphertext,
                                              const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> kyber_1024_decapsulate(const std::vector<uint8_t>& ciphertext,
                                               const std::vector<uint8_t>& private_key);

    // Dilithium Implementation
    std::vector<uint8_t> dilithium_2_generate_keypair();
    std::vector<uint8_t> dilithium_3_generate_keypair();
    std::vector<uint8_t> dilithium_5_generate_keypair();
    
    std::vector<uint8_t> dilithium_2_sign(const std::vector<uint8_t>& message,
                                         const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> dilithium_3_sign(const std::vector<uint8_t>& message,
                                         const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> dilithium_5_sign(const std::vector<uint8_t>& message,
                                         const std::vector<uint8_t>& private_key);
    
    bool dilithium_2_verify(const std::vector<uint8_t>& message,
                           const std::vector<uint8_t>& signature,
                           const std::vector<uint8_t>& public_key);
    bool dilithium_3_verify(const std::vector<uint8_t>& message,
                           const std::vector<uint8_t>& signature,
                           const std::vector<uint8_t>& public_key);
    bool dilithium_5_verify(const std::vector<uint8_t>& message,
                           const std::vector<uint8_t>& signature,
                           const std::vector<uint8_t>& public_key);

    // Falcon Implementation
    std::vector<uint8_t> falcon_512_generate_keypair();
    std::vector<uint8_t> falcon_1024_generate_keypair();
    
    std::vector<uint8_t> falcon_512_sign(const std::vector<uint8_t>& message,
                                        const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> falcon_1024_sign(const std::vector<uint8_t>& message,
                                         const std::vector<uint8_t>& private_key);
    
    bool falcon_512_verify(const std::vector<uint8_t>& message,
                          const std::vector<uint8_t>& signature,
                          const std::vector<uint8_t>& public_key);
    bool falcon_1024_verify(const std::vector<uint8_t>& message,
                           const std::vector<uint8_t>& signature,
                           const std::vector<uint8_t>& public_key);

    // SPHINCS+ Implementation
    std::vector<uint8_t> sphincs_sha256_128f_simple_generate_keypair();
    std::vector<uint8_t> sphincs_sha256_192f_simple_generate_keypair();
    std::vector<uint8_t> sphincs_sha256_256f_simple_generate_keypair();
    
    std::vector<uint8_t> sphincs_sha256_128f_simple_sign(const std::vector<uint8_t>& message,
                                                        const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> sphincs_sha256_192f_simple_sign(const std::vector<uint8_t>& message,
                                                        const std::vector<uint8_t>& private_key);
    std::vector<uint8_t> sphincs_sha256_256f_simple_sign(const std::vector<uint8_t>& message,
                                                        const std::vector<uint8_t>& private_key);
    
    bool sphincs_sha256_128f_simple_verify(const std::vector<uint8_t>& message,
                                          const std::vector<uint8_t>& signature,
                                          const std::vector<uint8_t>& public_key);
    bool sphincs_sha256_192f_simple_verify(const std::vector<uint8_t>& message,
                                          const std::vector<uint8_t>& signature,
                                          const std::vector<uint8_t>& public_key);
    bool sphincs_sha256_256f_simple_verify(const std::vector<uint8_t>& message,
                                          const std::vector<uint8_t>& signature,
                                          const std::vector<uint8_t>& public_key);

    // Helper Functions
    std::vector<uint8_t> generate_random_bytes(size_t length);
    std::vector<uint8_t> compute_hash(const std::vector<uint8_t>& data);
    std::vector<uint8_t> compute_hmac(const std::vector<uint8_t>& key,
                                     const std::vector<uint8_t>& data);
    
    // Classical Encryption for Hybrid Schemes
    std::vector<uint8_t> aes_256_gcm_encrypt(const std::vector<uint8_t>& key,
                                            const std::vector<uint8_t>& nonce,
                                            const std::vector<uint8_t>& plaintext,
                                            const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> aes_256_gcm_decrypt(const std::vector<uint8_t>& key,
                                            const std::vector<uint8_t>& nonce,
                                            const std::vector<uint8_t>& ciphertext,
                                            const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> chacha20_poly1305_encrypt(const std::vector<uint8_t>& key,
                                                   const std::vector<uint8_t>& nonce,
                                                   const std::vector<uint8_t>& plaintext,
                                                   const std::vector<uint8_t>& aad);
    
    std::vector<uint8_t> chacha20_poly1305_decrypt(const std::vector<uint8_t>& key,
                                                   const std::vector<uint8_t>& nonce,
                                                   const std::vector<uint8_t>& ciphertext,
                                                   const std::vector<uint8_t>& aad);

    // NTT (Number Theoretic Transform) for Lattice-based algorithms
    std::vector<uint8_t> ntt_transform(const std::vector<uint8_t>& data);
    std::vector<uint8_t> ntt_inverse_transform(const std::vector<uint8_t>& data);
    
    // Polynomial operations for lattice-based algorithms
    std::vector<uint8_t> polynomial_add(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
    std::vector<uint8_t> polynomial_multiply(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
    std::vector<uint8_t> polynomial_reduce(const std::vector<uint8_t>& poly, uint32_t modulus);
    
    // Hash functions
    std::vector<uint8_t> sha256(const std::vector<uint8_t>& data);
    std::vector<uint8_t> sha512(const std::vector<uint8_t>& data);
    std::vector<uint8_t> shake128(const std::vector<uint8_t>& data, size_t output_length);
    std::vector<uint8_t> shake256(const std::vector<uint8_t>& data, size_t output_length);
    
    // XOF (Extendable Output Function)
    std::vector<uint8_t> xof(const std::vector<uint8_t>& data, size_t output_length);
    
    // Random Oracle
    std::vector<uint8_t> random_oracle(const std::vector<uint8_t>& input, size_t output_length);
    
    // Encoding/Decoding
    std::vector<uint8_t> encode_public_key(const std::vector<uint8_t>& key, PQCAlgorithm algorithm);
    std::vector<uint8_t> decode_public_key(const std::vector<uint8_t>& encoded, PQCAlgorithm algorithm);
    std::vector<uint8_t> encode_private_key(const std::vector<uint8_t>& key, PQCAlgorithm algorithm);
    std::vector<uint8_t> decode_private_key(const std::vector<uint8_t>& encoded, PQCAlgorithm algorithm);
    std::vector<uint8_t> encode_signature(const std::vector<uint8_t>& signature, PQCAlgorithm algorithm);
    std::vector<uint8_t> decode_signature(const std::vector<uint8_t>& encoded, PQCAlgorithm algorithm);
    std::vector<uint8_t> encode_ciphertext(const std::vector<uint8_t>& ciphertext, PQCAlgorithm algorithm);
    std::vector<uint8_t> decode_ciphertext(const std::vector<uint8_t>& encoded, PQCAlgorithm algorithm);
};

} // namespace sonet::pqc