import type { ReactNode } from 'react';
import { AuthProvider } from './auth';
import { PlaceProvider } from './place';
import { SettingsProvider } from './settings';
import { ToastProvider } from './toast';
import { WeatherProvider } from './weather';

export function Providers({ children }: { children: ReactNode }) {
  return (
    <SettingsProvider>
      <ToastProvider>
        <AuthProvider>
          <PlaceProvider>
            <WeatherProvider>{children}</WeatherProvider>
          </PlaceProvider>
        </AuthProvider>
      </ToastProvider>
    </SettingsProvider>
  );
}
