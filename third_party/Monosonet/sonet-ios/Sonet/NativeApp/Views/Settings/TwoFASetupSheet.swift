import SwiftUI
import CoreImage.CIFilterBuiltins

struct TwoFASetupSheet: View {
    @ObservedObject var viewModel: TwoFactorViewModel
    @Environment(\.dismiss) private var dismiss
    @State private var verificationCode = ""
    @State private var currentStep: SetupStep = .qrCode
    @State private var qrCodeImage: UIImage?
    @State private var secretKey = ""
    @State private var isLoading = false
    @State private var error: String?
    
    enum SetupStep: CaseIterable {
        case qrCode, verification, backupCodes, complete
    }
    
    var body: some View {
        NavigationView {
            VStack(spacing: 24) {
                // Progress Indicator
                ProgressView(value: Double(SetupStep.allCases.firstIndex(of: currentStep) ?? 0), total: Double(SetupStep.allCases.count - 1))
                    .progressViewStyle(LinearProgressViewStyle())
                    .padding(.horizontal)
                
                // Step Content
                switch currentStep {
                case .qrCode:
                    QRCodeStepView(
                        qrCodeImage: $qrCodeImage,
                        secretKey: $secretKey,
                        isLoading: $isLoading,
                        onGenerateQR: generateQRCode
                    )
                    
                case .verification:
                    VerificationStepView(
                        verificationCode: $verificationCode,
                        isLoading: $isLoading,
                        error: $error,
                        onVerify: verifyCode
                    )
                    
                case .backupCodes:
                    BackupCodesStepView(
                        backupCodes: viewModel.backupCodes,
                        onGenerate: generateBackupCodes,
                        onComplete: completeSetup
                    )
                    
                case .complete:
                    CompleteStepView(onFinish: dismiss)
                }
                
                Spacer()
                
                // Navigation Buttons
                HStack {
                    if currentStep != .qrCode {
                        Button("Back") {
                            goToPreviousStep()
                        }
                        .buttonStyle(.bordered)
                    }
                    
                    Spacer()
                    
                    if currentStep != .complete {
                        Button(currentStep == .backupCodes ? "Skip" : "Next") {
                            if currentStep == .backupCodes {
                                completeSetup()
                            } else {
                                goToNextStep()
                            }
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(!canProceed)
                    }
                }
                .padding(.horizontal)
            }
            .padding()
            .navigationTitle("Set Up 2FA")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
            }
        }
        .onAppear {
            generateQRCode()
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") {
                viewModel.clearError()
            }
        } message: {
            if let error = viewModel.error {
                Text(error)
            }
        }
    }
    
    // MARK: - Computed Properties
    private var canProceed: Bool {
        switch currentStep {
        case .qrCode:
            return qrCodeImage != nil
        case .verification:
            return verificationCode.count == 6
        case .backupCodes:
            return true
        case .complete:
            return true
        }
    }
    
    // MARK: - Navigation Methods
    private func goToNextStep() {
        guard let currentIndex = SetupStep.allCases.firstIndex(of: currentStep),
              currentIndex + 1 < SetupStep.allCases.count else { return }
        
        currentStep = SetupStep.allCases[currentIndex + 1]
    }
    
    private func goToPreviousStep() {
        guard let currentIndex = SetupStep.allCases.firstIndex(of: currentStep),
              currentIndex > 0 else { return }
        
        currentStep = SetupStep.allCases[currentIndex - 1]
    }
    
    // MARK: - Setup Methods
    private func generateQRCode() {
        isLoading = true
        error = nil
        
        Task {
            do {
                let request = Setup2FARequest.newBuilder()
                    .setUserId("current_user")
                    .build()
                
                let response = try await viewModel.grpcClient.setup2FA(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.secretKey = response.secretKey
                        self.qrCodeImage = generateQRCodeImage(from: response.qrCodeUrl)
                        self.isLoading = false
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.isLoading = false
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to generate QR code: \(error.localizedDescription)"
                    self.isLoading = false
                }
            }
        }
    }
    
    private func verifyCode() {
        guard verificationCode.count == 6 else { return }
        
        isLoading = true
        error = nil
        
        Task {
            do {
                let request = Verify2FARequest.newBuilder()
                    .setUserId("current_user")
                    .setVerificationCode(verificationCode)
                    .setSecretKey(secretKey)
                    .build()
                
                let response = try await viewModel.grpcClient.verify2FA(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.isLoading = false
                        self.goToNextStep()
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.isLoading = false
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to verify code: \(error.localizedDescription)"
                    self.isLoading = false
                }
            }
        }
    }
    
    private func generateBackupCodes() {
        viewModel.generateBackupCodes()
    }
    
    private func completeSetup() {
        Task {
            await MainActor.run {
                viewModel.is2FAEnabled = true
                viewModel.save2FAStatus()
                goToNextStep()
            }
        }
    }
    
    // MARK: - Helper Methods
    private func generateQRCodeImage(from urlString: String) -> UIImage? {
        guard let url = URL(string: urlString) else { return nil }
        
        let context = CIContext()
        let filter = CIFilter.qrCodeGenerator()
        
        filter.message = Data(urlString.utf8)
        filter.correctionLevel = "M"
        
        if let outputImage = filter.outputImage {
            let transform = CGAffineTransform(scaleX: 10, y: 10)
            let scaledImage = outputImage.transformed(by: transform)
            
            if let cgImage = context.createCGImage(scaledImage, from: scaledImage.extent) {
                return UIImage(cgImage: cgImage)
            }
        }
        
        return nil
    }
}

// MARK: - QR Code Step View
struct QRCodeStepView: View {
    @Binding var qrCodeImage: UIImage?
    @Binding var secretKey: String
    @Binding var isLoading: Bool
    let onGenerateQR: () -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            Text("Scan QR Code")
                .font(.title2)
                .fontWeight(.semibold)
            
            Text("Open your authenticator app and scan this QR code to add Sonet to your 2FA app.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
            
            if isLoading {
                ProgressView()
                    .scaleEffect(1.5)
            } else if let qrImage = qrCodeImage {
                Image(uiImage: qrImage)
                    .interpolation(.none)
                    .resizable()
                    .scaledToFit()
                    .frame(width: 200, height: 200)
                    .background(Color.white)
                    .cornerRadius(12)
                    .shadow(radius: 4)
            } else {
                Button("Generate QR Code") {
                    onGenerateQR()
                }
                .buttonStyle(.borderedProminent)
            }
            
            if !secretKey.isEmpty {
                VStack(spacing: 8) {
                    Text("Manual Entry Key")
                        .font(.headline)
                    
                    Text(secretKey)
                        .font(.system(.body, design: .monospaced))
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(8)
                        .textSelection(.enabled)
                }
            }
            
            Text("If you can't scan the QR code, you can manually enter the key above into your authenticator app.")
                .font(.caption)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
        }
        .padding()
    }
}

// MARK: - Verification Step View
struct VerificationStepView: View {
    @Binding var verificationCode: String
    @Binding var isLoading: Bool
    @Binding var error: String?
    let onVerify: () -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            Text("Enter Verification Code")
                .font(.title2)
                .fontWeight(.semibold)
            
            Text("Enter the 6-digit code from your authenticator app to verify 2FA setup.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
            
            HStack(spacing: 12) {
                ForEach(0..<6, id: \.self) { index in
                    VerificationCodeDigit(
                        digit: index < verificationCode.count ? String(verificationCode[verificationCode.index(verificationCode.startIndex, offsetBy: index)]) : "",
                        isActive: index == verificationCode.count
                    )
                }
            }
            
            TextField("000000", text: $verificationCode)
                .keyboardType(.numberPad)
                .textFieldStyle(.roundedBorder)
                .onChange(of: verificationCode) { newValue in
                    // Limit to 6 digits
                    if newValue.count > 6 {
                        verificationCode = String(newValue.prefix(6))
                    }
                    // Only allow numbers
                    verificationCode = newValue.filter { $0.isNumber }
                }
                .opacity(0) // Hidden but functional
            
            if let error = error {
                Text(error)
                    .foregroundColor(.red)
                    .font(.caption)
            }
            
            Button("Verify") {
                onVerify()
            }
            .buttonStyle(.borderedProminent)
            .disabled(verificationCode.count != 6 || isLoading)
        }
        .padding()
    }
}

// MARK: - Verification Code Digit
struct VerificationCodeDigit: View {
    let digit: String
    let isActive: Bool
    
    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 8)
                .fill(isActive ? Color.blue.opacity(0.2) : Color(.systemGray5))
                .frame(width: 50, height: 60)
            
            if digit.isEmpty {
                Text("•")
                    .font(.title)
                    .foregroundColor(.secondary)
            } else {
                Text(digit)
                    .font(.title2)
                    .fontWeight(.semibold)
            }
        }
    }
}

// MARK: - Backup Codes Step View
struct BackupCodesStepView: View {
    let backupCodes: [String]
    let onGenerate: () -> Void
    let onComplete: () -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            Text("Backup Codes")
                .font(.title2)
                .fontWeight(.semibold)
            
            Text("Save these backup codes in a secure location. You can use them to access your account if you lose your 2FA device.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
            
            if backupCodes.isEmpty {
                Button("Generate Backup Codes") {
                    onGenerate()
                }
                .buttonStyle(.borderedProminent)
            } else {
                VStack(spacing: 16) {
                    ForEach(backupCodes, id: \.self) { code in
                        HStack {
                            Text(code)
                                .font(.system(.body, design: .monospaced))
                                .fontWeight(.medium)
                            Spacer()
                            Button("Copy") {
                                UIPasteboard.general.string = code
                            }
                            .buttonStyle(.bordered)
                        }
                        .padding()
                        .background(Color(.systemGray6))
                        .cornerRadius(8)
                    }
                    
                    Text("⚠️ Save these codes now! They won't be shown again.")
                        .font(.caption)
                        .foregroundColor(.orange)
                        .multilineTextAlignment(.center)
                }
            }
        }
        .padding()
    }
}

// MARK: - Complete Step View
struct CompleteStepView: View {
    let onFinish: () -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            Image(systemName: "checkmark.shield.fill")
                .font(.system(size: 80))
                .foregroundColor(.green)
            
            Text("Two-Factor Authentication Enabled!")
                .font(.title2)
                .fontWeight(.semibold)
            
            Text("Your account is now protected with an additional layer of security. You'll need to enter a verification code from your authenticator app each time you sign in.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
            
            Button("Finish") {
                onFinish()
            }
            .buttonStyle(.borderedProminent)
        }
        .padding()
    }
}

// MARK: - gRPC Request/Response Extensions (Placeholder)
extension Setup2FARequest {
    static func newBuilder() -> Setup2FARequestBuilder {
        return Setup2FARequestBuilder()
    }
}

extension Verify2FARequest {
    static func newBuilder() -> Verify2FARequestBuilder {
        return Verify2FARequestBuilder()
    }
}

// MARK: - Request Builders (Placeholder - replace with actual gRPC types)
class Setup2FARequestBuilder {
    private var userId: String = ""
    
    func setUserId(_ userId: String) -> Setup2FARequestBuilder {
        self.userId = userId
        return self
    }
    
    func build() -> Setup2FARequest {
        return Setup2FARequest(userId: userId)
    }
}

class Verify2FARequestBuilder {
    private var userId: String = ""
    private var verificationCode: String = ""
    private var secretKey: String = ""
    
    func setUserId(_ userId: String) -> Verify2FARequestBuilder {
        self.userId = userId
        return self
    }
    
    func setVerificationCode(_ code: String) -> Verify2FARequestBuilder {
        self.verificationCode = code
        return self
    }
    
    func setSecretKey(_ key: String) -> Verify2FARequestBuilder {
        self.secretKey = key
        return self
    }
    
    func build() -> Verify2FARequest {
        return Verify2FARequest(userId: userId, verificationCode: verificationCode, secretKey: secretKey)
    }
}

// MARK: - Request/Response Models (Placeholder - replace with actual gRPC types)
struct Setup2FARequest {
    let userId: String
}

struct Verify2FARequest {
    let userId: String
    let verificationCode: String
    let secretKey: String
}

struct Setup2FAResponse {
    let success: Bool
    let secretKey: String
    let qrCodeUrl: String
    let errorMessage: String
}

struct Verify2FAResponse {
    let success: Bool
    let errorMessage: String
}