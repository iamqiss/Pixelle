import UIKit
import Photos
import PhotosUI
import AVFoundation

class CameraExtensionManager: NSObject, ObservableObject {
    static let shared = CameraExtensionManager()
    
    @Published var isCameraAvailable = false
    @Published var isPhotoLibraryAvailable = false
    
    private override init() {
        super.init()
        checkPermissions()
    }
    
    // MARK: - Permission Checks
    private func checkPermissions() {
        checkCameraPermission()
        checkPhotoLibraryPermission()
    }
    
    private func checkCameraPermission() {
        switch AVCaptureDevice.authorizationStatus(for: .video) {
        case .authorized:
            isCameraAvailable = true
        case .notDetermined:
            AVCaptureDevice.requestAccess(for: .video) { [weak self] granted in
                DispatchQueue.main.async {
                    self?.isCameraAvailable = granted
                }
            }
        case .denied, .restricted:
            isCameraAvailable = false
        @unknown default:
            isCameraAvailable = false
        }
    }
    
    private func checkPhotoLibraryPermission() {
        switch PHPhotoLibrary.authorizationStatus() {
        case .authorized, .limited:
            isPhotoLibraryAvailable = true
        case .notDetermined:
            PHPhotoLibrary.requestAuthorization { [weak self] status in
                DispatchQueue.main.async {
                    self?.isPhotoLibraryAvailable = status == .authorized || status == .limited
                }
            }
        case .denied, .restricted:
            isPhotoLibraryAvailable = false
        @unknown default:
            isPhotoLibraryAvailable = false
        }
    }
    
    // MARK: - Camera Integration
    func presentCamera(from viewController: UIViewController, completion: @escaping (UIImage?) -> Void) {
        guard isCameraAvailable else {
            showCameraPermissionAlert(from: viewController)
            return
        }
        
        let imagePicker = UIImagePickerController()
        imagePicker.sourceType = .camera
        imagePicker.delegate = CameraDelegate(completion: completion)
        
        viewController.present(imagePicker, animated: true)
    }
    
    func presentPhotoLibrary(from viewController: UIViewController, completion: @escaping ([UIImage]) -> Void) {
        guard isPhotoLibraryAvailable else {
            showPhotoLibraryPermissionAlert(from: viewController)
            return
        }
        
        if #available(iOS 14.0, *) {
            presentPHPicker(from: viewController, completion: completion)
        } else {
            presentUIImagePicker(from: viewController, completion: completion)
        }
    }
    
    @available(iOS 14.0, *)
    private func presentPHPicker(from viewController: UIViewController, completion: @escaping ([UIImage]) -> Void) {
        var configuration = PHPickerConfiguration(photoLibrary: .shared())
        configuration.selectionLimit = 10
        configuration.filter = .images
        
        let picker = PHPickerViewController(configuration: configuration)
        picker.delegate = PHPickerDelegate(completion: completion)
        
        viewController.present(picker, animated: true)
    }
    
    private func presentUIImagePicker(from viewController: UIViewController, completion: @escaping ([UIImage]) -> Void) {
        let imagePicker = UIImagePickerController()
        imagePicker.sourceType = .photoLibrary
        imagePicker.delegate = PhotoLibraryDelegate(completion: completion)
        
        viewController.present(imagePicker, animated: true)
    }
    
    // MARK: - Direct Post to Sonet
    func postImageDirectlyToSonet(_ image: UIImage, from viewController: UIViewController) {
        // Save image to shared container for Sonet app
        if let imageData = image.jpegData(compressionQuality: 0.8) {
            saveImageToSharedContainer(imageData) { [weak self] success in
                if success {
                    self?.openSonetWithImage(image)
                } else {
                    self?.showErrorAlert(from: viewController)
                }
            }
        }
    }
    
    private func saveImageToSharedContainer(_ imageData: Data, completion: @escaping (Bool) -> Void) {
        guard let containerURL = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: "group.app.bsky") else {
            completion(false)
            return
        }
        
        let imageURL = containerURL.appendingPathComponent("sonet_camera_image.jpg")
        
        do {
            try imageData.write(to: imageURL)
            completion(true)
        } catch {
            print("Failed to save image: \(error)")
            completion(false)
        }
    }
    
    private func openSonetWithImage(_ image: UIImage) {
        // Open Sonet app with the saved image
        if let url = URL(string: "sonet://compose?image=sonet_camera_image.jpg") {
            if UIApplication.shared.canOpenURL(url) {
                UIApplication.shared.open(url)
            }
        }
    }
    
    // MARK: - Alerts
    private func showCameraPermissionAlert(from viewController: UIViewController) {
        let alert = UIAlertController(
            title: "Camera Access Required",
            message: "Please enable camera access in Settings to use this feature.",
            preferredStyle: .alert
        )
        
        alert.addAction(UIAlertAction(title: "Settings", style: .default) { _ in
            if let settingsURL = URL(string: UIApplication.openSettingsURLString) {
                UIApplication.shared.open(settingsURL)
            }
        })
        
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        
        viewController.present(alert, animated: true)
    }
    
    private func showPhotoLibraryPermissionAlert(from viewController: UIViewController) {
        let alert = UIAlertController(
            title: "Photo Library Access Required",
            message: "Please enable photo library access in Settings to use this feature.",
            preferredStyle: .alert
        )
        
        alert.addAction(UIAlertAction(title: "Settings", style: .default) { _ in
            if let settingsURL = URL(string: UIApplication.openSettingsURLString) {
                UIApplication.shared.open(settingsURL)
            }
        })
        
        alert.addAction(UIAlertAction(title: "Cancel", style: .cancel))
        
        viewController.present(alert, animated: true)
    }
    
    private func showErrorAlert(from viewController: UIViewController) {
        let alert = UIAlertController(
            title: "Error",
            message: "Failed to save image. Please try again.",
            preferredStyle: .alert
        )
        
        alert.addAction(UIAlertAction(title: "OK", style: .default))
        
        viewController.present(alert, animated: true)
    }
}

// MARK: - Camera Delegate
class CameraDelegate: NSObject, UIImagePickerControllerDelegate, UINavigationControllerDelegate {
    private let completion: (UIImage?) -> Void
    
    init(completion: @escaping (UIImage?) -> Void) {
        self.completion = completion
    }
    
    func imagePickerController(_ picker: UIImagePickerController, didFinishPickingMediaWithInfo info: [UIImagePickerController.InfoKey : Any]) {
        let image = info[.originalImage] as? UIImage
        picker.dismiss(animated: true) {
            self.completion(image)
        }
    }
    
    func imagePickerControllerDidCancel(_ picker: UIImagePickerController) {
        picker.dismiss(animated: true) {
            self.completion(nil)
        }
    }
}

// MARK: - Photo Library Delegate
class PhotoLibraryDelegate: NSObject, UIImagePickerControllerDelegate, UINavigationControllerDelegate {
    private let completion: ([UIImage]) -> Void
    
    init(completion: @escaping ([UIImage]) -> Void) {
        self.completion = completion
    }
    
    func imagePickerController(_ picker: UIImagePickerController, didFinishPickingMediaWithInfo info: [UIImagePickerController.InfoKey : Any]) {
        let image = info[.originalImage] as? UIImage
        picker.dismiss(animated: true) {
            if let image = image {
                self.completion([image])
            } else {
                self.completion([])
            }
        }
    }
    
    func imagePickerControllerDidCancel(_ picker: UIImagePickerController) {
        picker.dismiss(animated: true) {
            self.completion([])
        }
    }
}

// MARK: - PHPicker Delegate
@available(iOS 14.0, *)
class PHPickerDelegate: NSObject, PHPickerViewControllerDelegate {
    private let completion: ([UIImage]) -> Void
    
    init(completion: @escaping ([UIImage]) -> Void) {
        self.completion = completion
    }
    
    func picker(_ picker: PHPickerViewController, didFinishPicking results: [PHPickerResult]) {
        picker.dismiss(animated: true)
        
        var images: [UIImage] = []
        let group = DispatchGroup()
        
        for result in results {
            group.enter()
            result.itemProvider.loadObject(ofClass: UIImage.self) { object, error in
                if let image = object as? UIImage {
                    images.append(image)
                }
                group.leave()
            }
        }
        
        group.notify(queue: .main) {
            self.completion(images)
        }
    }
}