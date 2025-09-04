"use client"
import React from 'react'

type AuthState = {
  token?: string
}

type AuthContextValue = {
  state: AuthState
  setToken: (t?: string) => void
}

const AuthContext = React.createContext<AuthContextValue | undefined>(undefined)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = React.useState<AuthState>({ token: undefined })

  React.useEffect(() => {
    const t = typeof window !== 'undefined' ? window.localStorage.getItem('sp.token') || undefined : undefined
    if (t) setState({ token: t })
  }, [])

  const setToken = React.useCallback((t?: string) => {
    setState({ token: t })
    if (typeof window !== 'undefined') {
      if (t) window.localStorage.setItem('sp.token', t)
      else window.localStorage.removeItem('sp.token')
    }
  }, [])

  return (
    <AuthContext.Provider value={{ state, setToken }}>{children}</AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = React.useContext(AuthContext)
  if (!ctx) throw new Error('AuthProvider missing')
  return ctx
}

