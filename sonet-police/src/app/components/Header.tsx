"use client"
import Link from 'next/link'
import { useAuth } from '@/lib/auth'

export function Header() {
  const { state, setToken } = useAuth()
  return (
    <header className="w-full border-b bg-white">
      <div className="max-w-7xl mx-auto px-4 h-14 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Link href="/" className="font-semibold">Sonet Police</Link>
          <nav className="flex items-center gap-3 text-sm text-gray-600">
            <Link href="/queues">Queues</Link>
            <Link href="/audit">Audit</Link>
          </nav>
        </div>
        <div className="flex items-center gap-2">
          <input
            className="border rounded px-2 py-1 text-sm"
            placeholder="paste JWT for dev"
            defaultValue={state.token}
            onBlur={e => setToken(e.target.value || undefined)}
          />
        </div>
      </div>
    </header>
  )
}

