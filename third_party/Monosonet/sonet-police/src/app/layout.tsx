import './globals.css'
import React from 'react'
import { AuthProvider } from '@/lib/auth'
import { Header } from '@/app/components/Header'

export const metadata = {
  title: 'Sonet Police',
  description: 'Moderation Dashboard',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AuthProvider>
          <Header />
          {children}
        </AuthProvider>
      </body>
    </html>
  )
}

